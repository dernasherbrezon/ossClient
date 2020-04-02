package ru.r2cloud.ossclient;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.eclipsesource.json.ParseException;

public class SelectelOssClient implements OssClient {

	private static final String USER_AGENT = "ossClient/2.0 (dernasherbrezon)";

	private static final Logger LOG = LoggerFactory.getLogger(SelectelOssClient.class);

	private String authUrl;
	private String user;
	private String key;
	private String containerName;
	private int timeout;
	private int retries;
	private long retryTimeoutMillis;

	private String authToken;
	private String baseUrl;
	private long validUntil;

	private HttpClient httpclient;

	public void start() {
		httpclient = HttpClient.newBuilder().version(Version.HTTP_1_1).followRedirects(Redirect.NORMAL).connectTimeout(Duration.ofMillis(timeout)).build();
	}

	@Override
	public void delete(String path) throws OssException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("deleting: {}", path);
		}
		executeWithRetry(currentRetry -> {
			HttpRequest request = createRequest(path).DELETE().build();
			HttpResponse<String> response = httpclient.send(request, BodyHandlers.ofString());
			if (LOG.isDebugEnabled()) {
				LOG.debug("response: {}", response.body());
			}
			if (response.statusCode() == 201 || response.statusCode() == 204) {
				// log only when retry happened
				if (currentRetry > 0) {
					LOG.info("deleted: {}", path);
				}
				return true;
			}
			if (response.statusCode() == 401) {
				resetAuthToken();
				return false;
			}

			throw new OssException(response.statusCode(), "unable to delete");
		}, path);
	}

	@Override
	public void submit(File file, String path) throws OssException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("submitting: {}", path);
		}
		executeWithRetry(currentRetry -> {
			HttpRequest request = createRequest(path).PUT(BodyPublishers.ofFile(file.toPath())).build();
			HttpResponse<String> response = httpclient.send(request, BodyHandlers.ofString());
			if (LOG.isDebugEnabled()) {
				LOG.debug("response: {}", response.body());
			}
			if (response.statusCode() == 201) {
				// log only when retry happened
				if (currentRetry > 0) {
					LOG.info("submitted: {}", path);
				}
				return true;
			}
			if (response.statusCode() == 401) {
				resetAuthToken();
			}
			return false;
		}, path);
	}

	private void executeWithRetry(RetryFunction toExecute, String path) throws OssException {
		int currentRetry = 0;
		while (!Thread.currentThread().isInterrupted()) {
			try {
				refreshToken();
				if (!toExecute.apply(currentRetry)) {
					currentRetry++;
				} else {
					break;
				}
			} catch (IOException e) {
				resetAuthToken();
				if (currentRetry < retries) {
					currentRetry++;
					LOG.info("unable to process: {} retry...{} exception {}", path, currentRetry, e.getMessage());
					try {
						Thread.sleep(retryTimeoutMillis);
					} catch (InterruptedException e1) {
						LOG.info("sleep interrupted. exit");
						Thread.currentThread().interrupt();
						break;
					}
					continue;
				}
				throw new OssException(OssException.INTERNAL_SERVER_ERROR, "unable to process", e);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
	}

	@Override
	public List<FileEntry> listFiles(ListRequest req) throws OssException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("listing: {}", req);
		}
		refreshToken();
		HttpRequest request = createRequest(createRequestUrl(req)).GET().build();
		try {
			HttpResponse<InputStream> response = httpclient.send(request, BodyHandlers.ofInputStream());
			if (response.statusCode() != 200) {
				LOG.info("invalid response: {}", response.statusCode());
				return Collections.emptyList();
			}
			return readEntries(response.body());
		} catch (Exception e) {
			LOG.error("unable to list files", e);
			return Collections.emptyList();
		}
	}

	private static List<FileEntry> readEntries(InputStream is) {
		JsonValue parsed;
		try {
			parsed = Json.parse(new InputStreamReader(is, StandardCharsets.UTF_8));
		} catch (ParseException e) {
			LOG.info("malformed json");
			return Collections.emptyList();
		} catch (IOException e) {
			LOG.info("unable to read data", e);
			return Collections.emptyList();
		}
		if (!parsed.isArray()) {
			LOG.info("not an array");
			return Collections.emptyList();
		}
		JsonArray array = parsed.asArray();
		List<FileEntry> result = new ArrayList<>(array.size());
		for (JsonValue cur : array) {
			if (!cur.isObject()) {
				continue;
			}
			result.add(convert(cur.asObject()));
		}
		return result;
	}

	private static FileEntry convert(JsonObject cur) {
		FileEntry result = new FileEntry();
		result.setBytes(cur.getLong("bytes", 0));
		result.setContentType(cur.getString("content_type", null));
		result.setHash(cur.getString("hash", null));
		result.setLastModified(cur.getString("last_modified", null));
		result.setName(cur.getString("name", null));
		return result;
	}

	private static String createRequestUrl(ListRequest req) {
		StringBuilder builder = new StringBuilder();
		builder.append("/?format=json");
		if (req.getLimit() > 0) {
			builder.append("&limit=").append(req.getLimit());
		}
		if (req.getMarker() != null) {
			builder.append("&marker=").append(URLEncoder.encode(req.getMarker(), StandardCharsets.UTF_8));
		}
		if (req.getPrefix() != null) {
			builder.append("&prefix=").append(req.getPrefix());
		}
		if (req.getPath() != null) {
			builder.append("&path=").append(req.getPath());
		}
		if (req.getDelimiter() != null) {
			builder.append("&delimiter=").append(req.getDelimiter());
		}
		return builder.toString();
	}

	@Override
	public void download(String path, Callback f) throws OssException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("downloading: {}", path);
		}
		refreshToken();
		HttpRequest request = createRequest(path).GET().build();
		HttpResponse<InputStream> response;
		try {
			response = httpclient.send(request, BodyHandlers.ofInputStream());
		} catch (IOException e) {
			throw new OssException(OssException.INTERNAL_SERVER_ERROR, "unable to process", e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
		if (response.statusCode() != 200) {
			throw new OssException(response.statusCode(), "unable to download: " + path);
		}

		f.onData(response.body());
	}

	private synchronized void refreshToken() throws OssException {
		if (authToken != null && System.currentTimeMillis() < validUntil) {
			return;
		}
		if (authToken != null) {
			LOG.info("re-newing auth token");
		}
		long start = System.currentTimeMillis();
		Builder result = HttpRequest.newBuilder().uri(URI.create(authUrl));
		result.timeout(Duration.ofMinutes(1L));
		result.header("User-Agent", USER_AGENT);
		result.header("X-Auth-User", user);
		result.header("X-Auth-Key", key);
		result.GET();

		try {
			HttpResponse<String> response = httpclient.send(result.build(), BodyHandlers.ofString());
			if (response.statusCode() != 204) {
				throw new OssException(response.statusCode(), "unable to authenticate");
			}
			authToken = response.headers().firstValue("X-Auth-Token").get();
			baseUrl = response.headers().firstValue("X-Storage-Url").get();

			LOG.info("baseurl: {}", baseUrl);
			// convert seconds to millis
			validUntil = (start + Long.valueOf(response.headers().firstValue("X-Expire-Auth-Token").get()) * 1000) - timeout;
			LOG.info("the token will expire at: {}", new Date(validUntil));
		} catch (IOException e) {
			throw new OssException(OssException.INTERNAL_SERVER_ERROR, "unable to read auth response", e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new OssException(OssException.INTERNAL_SERVER_ERROR, "interrupted");
		}
	}

	private synchronized HttpRequest.Builder createRequest(String path) {
		Builder result = HttpRequest.newBuilder().uri(URI.create(baseUrl + "/" + containerName + path));
		result.timeout(Duration.ofMillis(timeout));
		result.header("User-Agent", USER_AGENT);
		result.header("X-Auth-Token", authToken);
		return result;
	}

	private synchronized void resetAuthToken() {
		LOG.info("not authorized. resetting auth token");
		authToken = null;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public void setContainerName(String containerName) {
		this.containerName = containerName;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public void setRetries(int retries) {
		this.retries = retries;
	}

	public void setRetryTimeoutMillis(long retryTimeoutMillis) {
		this.retryTimeoutMillis = retryTimeoutMillis;
	}

	public void setAuthUrl(String authUrl) {
		this.authUrl = authUrl;
	}

}

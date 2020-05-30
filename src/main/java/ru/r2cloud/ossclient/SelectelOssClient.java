package ru.r2cloud.ossclient;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.eclipsesource.json.ParseException;

public class SelectelOssClient implements OssClient {

	private static final Logger LOG = LoggerFactory.getLogger(SelectelOssClient.class);

	private static String userAgent;
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

	private CloseableHttpClient httpclient;

	static {
		String version = readVersion();
		if (version == null) {
			version = "2.0";
		}
		userAgent = "ossClient/" + version + " (dernasherbrezon)";
	}
	
	public void start() {
		RequestConfig config = RequestConfig.custom().setConnectTimeout(timeout).setConnectionRequestTimeout(timeout).build();
		httpclient = HttpClientBuilder.create().setUserAgent(userAgent).setDefaultRequestConfig(config).build();
	}

	@Override
	public void delete(String path) throws OssException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("deleting: {}", path);
		}
		executeWithRetry(currentRetry -> {
			HttpDelete method = new HttpDelete(baseUrl + "/" + containerName + path);
			method.setHeader("X-Auth-Token", authToken);
			org.apache.http.HttpResponse response = null;
			try {
				response = httpclient.execute(method);
				if (LOG.isDebugEnabled()) {
					LOG.debug("response: {}", EntityUtils.toString(response.getEntity()));
				}
				int statusCode = response.getStatusLine().getStatusCode();
				if (statusCode == 201 || statusCode == 204) {
					// log only when retry happened
					if (currentRetry > 0) {
						LOG.info("deleted: {}", path);
					}
					return true;
				}
				if (statusCode == 401) {
					resetAuthToken();
					return false;
				}
				throw new OssException(statusCode, "unable to delete");
			} finally {
				if (response != null) {
					EntityUtils.consumeQuietly(response.getEntity());
				}
			}
		}, path);
	}

	@Override
	public void submit(File file, String path) throws OssException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("submitting: {}", path);
		}
		executeWithRetry(currentRetry -> {
			HttpPut method = new HttpPut(baseUrl + "/" + containerName + path);
			method.setHeader("X-Auth-Token", authToken);
			method.setEntity(new FileEntity(file));
			org.apache.http.HttpResponse response = null;
			try {
				response = httpclient.execute(method);
				if (LOG.isDebugEnabled()) {
					LOG.debug("response: {}", EntityUtils.toString(response.getEntity()));
				}
				int statusCode = response.getStatusLine().getStatusCode();
				if (statusCode == 201) {
					// log only when retry happened
					if (currentRetry > 0) {
						LOG.info("submitted: {}", path);
					}
					return true;
				}
				if (statusCode == 401) {
					resetAuthToken();
					return false;
				}
				return false;
			} finally {
				if (response != null) {
					EntityUtils.consumeQuietly(response.getEntity());
				}
			}
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
		HttpGet method = new HttpGet(baseUrl + "/" + containerName + createRequestUrl(req));
		method.setHeader("X-Auth-Token", authToken);
		org.apache.http.HttpResponse response = null;
		try {
			response = httpclient.execute(method);
			int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode != 200) {
				LOG.info("invalid response: {}", statusCode);
				return Collections.emptyList();
			}
			return readEntries(response.getEntity().getContent());
		} catch (Exception e) {
			LOG.error("unable to list files", e);
			return Collections.emptyList();
		} finally {
			if (response != null) {
				EntityUtils.consumeQuietly(response.getEntity());
			}
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
		HttpGet method = new HttpGet(baseUrl + "/" + containerName + path);
		method.setHeader("X-Auth-Token", authToken);
		org.apache.http.HttpResponse response = null;
		try {
			response = httpclient.execute(method);
			int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode != 200) {
				throw new OssException(statusCode, "unable to download: " + path);
			}
			f.onData(response.getEntity().getContent());
		} catch (IOException e) {
			throw new OssException(OssException.INTERNAL_SERVER_ERROR, "unable to process", e);
		} finally {
			if (response != null) {
				EntityUtils.consumeQuietly(response.getEntity());
			}
		}
	}

	private synchronized void refreshToken() throws OssException {
		if (authToken != null && System.currentTimeMillis() < validUntil) {
			return;
		}
		if (authToken != null) {
			LOG.info("re-newing auth token");
		}
		long start = System.currentTimeMillis();
		HttpGet method = new HttpGet(authUrl);
		method.setHeader("X-Auth-User", user);
		method.setHeader("X-Auth-Key", key);
		org.apache.http.HttpResponse response = null;
		try {
			response = httpclient.execute(method);
			int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode != 204) {
				throw new OssException(statusCode, "unable to authenticate");
			}
			authToken = response.getFirstHeader("X-Auth-Token").getValue();
			baseUrl = response.getFirstHeader("X-Storage-Url").getValue();

			LOG.info("baseurl: {}", baseUrl);
			// convert seconds to millis
			validUntil = (start + Long.valueOf(response.getFirstHeader("X-Expire-Auth-Token").getValue()) * 1000) - timeout;
			LOG.info("the token will expire at: {}", new Date(validUntil));
		} catch (IOException e) {
			throw new OssException(OssException.INTERNAL_SERVER_ERROR, "unable to read auth response", e);
		} finally {
			if (response != null) {
				EntityUtils.consumeQuietly(response.getEntity());
			}
		}
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

	private static String readVersion() {
		try (InputStream is = SelectelOssClient.class.getClassLoader().getResourceAsStream("META-INF/maven/ru.r2cloud/ossClient/pom.properties")) {
			if (is != null) {
				Properties p = new Properties();
				p.load(is);
				return p.getProperty("version", null);
			}
			return null;
		} catch (IOException e) {
			return null;
		}
	}
}

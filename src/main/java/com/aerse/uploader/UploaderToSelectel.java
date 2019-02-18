package com.aerse.uploader;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.RequestAcceptEncoding;
import org.apache.http.client.protocol.ResponseContentEncoding;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class UploaderToSelectel implements Uploader {

	private static final Logger LOG = LoggerFactory.getLogger(UploaderToSelectel.class);
	private static final String INTERNAL_SERVER_ERROR = "Внутренняя ошибка. Попробуйте позднее";

	private String user;
	private String key;
	private String containerName;
	private int timeout;
	private int retries;
	private long retryTimeoutMillis;

	private String authToken;
	private String baseUrl;
	private long validUntil;

	private CloseableHttpClient client;

	@PostConstruct
	public void start() {
		client = createClient();
	}

	@PreDestroy
	public void stop() throws IOException {
		if (client != null) {
			client.close();
		}
	}

	@Override
	public void delete(String path) throws UploadException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("deleting: {}", path);
		}
		int retryCount = 0;
		while (true) {
			HttpResponse result = null;
			try {
				String authToken = getAuthToken();
				HttpDelete del = new HttpDelete(baseUrl + "/" + containerName + path);
				del.addHeader("X-Auth-Token", authToken);
				result = client.execute(del);
				if (LOG.isDebugEnabled() && result.getEntity() != null) {
					LOG.debug("response: {}", EntityUtils.toString(result.getEntity()));
				}
				if (result.getStatusLine().getStatusCode() == HttpStatus.SC_NO_CONTENT) {
					return;
				}
				if (result.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
					retryCount++;
					resetAuthToken();
					continue;
				}
				throw new UploadException(result.getStatusLine().getStatusCode(), INTERNAL_SERVER_ERROR);
			} catch (IOException e) {
				if (retryCount < retries) {
					retryCount++;
					LOG.info("unable to delete: {} retry...{} exception {}", e.getMessage(), retryCount, e.getMessage());
					try {
						Thread.sleep(retryTimeoutMillis);
					} catch (InterruptedException e1) {
						LOG.info("sleep interrupted. exit");
						break;
					}
					continue;
				}
				throw new UploadException(UploadException.INTERNAL_SERVER_ERROR, INTERNAL_SERVER_ERROR, e);
			} finally {
				if (result != null) {
					EntityUtils.consumeQuietly(result.getEntity());
				}
			}
		}
	}

	@Override
	public void submit(File file, String path) throws UploadException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("submitting: {}", path);
		}
		int retryCount = 0;
		while (true) {
			HttpResponse result = null;
			try {
				String authToken = getAuthToken();
				HttpPut put = new HttpPut(baseUrl + "/" + containerName + path);
				put.addHeader("X-Auth-Token", authToken);
				put.setEntity(new FileEntity(file));
				result = client.execute(put);
				if (LOG.isDebugEnabled() && result.getEntity() != null) {
					LOG.debug("response: {}", EntityUtils.toString(result.getEntity()));
				}
				if (result.getStatusLine().getStatusCode() == HttpStatus.SC_CREATED) {
					if (retryCount > 0) {
						LOG.info("submitted: {}", path);
					}
					return;
				}
				if (result.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
					retryCount++;
					resetAuthToken();
					continue;
				}
				throw new UploadException(result.getStatusLine().getStatusCode(), "invalid response: " + result.getStatusLine());
			} catch (IOException e) {
				if (retryCount < 3) {
					retryCount++;
					LOG.info("unable to submit: {} retry...{} exception {}", path, retryCount, e.getMessage());
					try {
						Thread.sleep(retryTimeoutMillis);
					} catch (InterruptedException e1) {
						LOG.info("sleep interrupted. exit");
						break;
					}
					continue;
				}
				throw new UploadException(UploadException.INTERNAL_SERVER_ERROR, INTERNAL_SERVER_ERROR, e);
			} finally {
				if (result != null) {
					EntityUtils.consumeQuietly(result.getEntity());
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<FileEntry> listFiles(ListRequest req) {
		if (LOG.isTraceEnabled()) {
			LOG.trace("listing: {}", req);
		}
		String authToken2 = getAuthToken();

		StringBuilder builder = new StringBuilder();
		builder.append(baseUrl).append("/").append(containerName).append("?format=json");
		if (req.getLimit() > 0) {
			builder.append("&limit=").append(req.getLimit());
		}
		if (req.getMarker() != null) {
			try {
				builder.append("&marker=").append(URLEncoder.encode(req.getMarker(), "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				return Collections.emptyList();
			}
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
		HttpGet m = new HttpGet(builder.toString());
		m.addHeader("X-Auth-Token", authToken2);
		HttpResponse result = null;
		try {
			result = client.execute(m);
			if (result.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				LOG.info("invalid response: {}", result.getStatusLine());
				return Collections.emptyList();
			}
			Type typeOfSrc = new TypeToken<List<FileEntry>>() {
				// do nothing
			}.getType();
			return (List<FileEntry>) new Gson().fromJson(new InputStreamReader(result.getEntity().getContent(), StandardCharsets.UTF_8), typeOfSrc);
		} catch (Exception e) {
			LOG.error("unable to list files", e);
			return Collections.emptyList();
		} finally {
			if (result != null) {
				EntityUtils.consumeQuietly(result.getEntity());
			}
		}
	}

	@Override
	public void download(String path, Callback f) {
		if (LOG.isTraceEnabled()) {
			LOG.trace("downloading: {}", path);
		}
		String authToken2 = getAuthToken();
		HttpGet put = new HttpGet(baseUrl + "/" + containerName + path);
		put.addHeader("X-Auth-Token", authToken2);
		HttpResponse result = null;
		try {
			result = client.execute(put);
			if (result.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				LOG.info("unable to download: {}", EntityUtils.toString(result.getEntity()));
			} else {
				f.onData(result.getEntity().getContent());
			}
		} catch (Exception e) {
			LOG.error("unable to download", e);
		} finally {
			if (result != null) {
				EntityUtils.consumeQuietly(result.getEntity());
			}
		}
	}

	private synchronized String getAuthToken() {
		if (authToken == null || System.currentTimeMillis() > validUntil) {
			if (authToken != null) {
				LOG.info("re-newing auth token");
			}
			long start = System.currentTimeMillis();
			HttpGet get = new HttpGet("https://auth.selcdn.ru");
			get.addHeader("X-Auth-User", user);
			get.addHeader("X-Auth-Key", key);
			HttpResponse response = null;
			try {
				response = client.execute(get);
				if (response.getStatusLine().getStatusCode() != HttpStatus.SC_NO_CONTENT) {
					LOG.info("unable to auth: {}", EntityUtils.toString(response.getEntity()));
					throw new RuntimeException(INTERNAL_SERVER_ERROR);
				}
				authToken = response.getFirstHeader("X-Auth-Token").getValue();
				baseUrl = response.getFirstHeader("X-Storage-Url").getValue();
				// convert seconds to millis
				validUntil = (start + Long.valueOf(response.getFirstHeader("X-Expire-Auth-Token").getValue()) * 1000) - timeout;
				LOG.info("the token will expire at: {}", new Date(validUntil));
			} catch (IOException e) {
				throw new RuntimeException(INTERNAL_SERVER_ERROR, e);
			} finally {
				if (response != null) {
					EntityUtils.consumeQuietly(response.getEntity());
				}
			}
		}
		return authToken;
	}

	private synchronized void resetAuthToken() {
		LOG.info("not authorized. resetting auth token");
		authToken = null;
	}

	private CloseableHttpClient createClient() {
		return createClient(timeout);
	}

	static CloseableHttpClient createClient(int timeout) {
		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();

		RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(timeout).setConnectionRequestTimeout(timeout).setSocketTimeout(timeout).build();
		HttpClientBuilder builder = HttpClientBuilder.create();
		builder.addInterceptorFirst(new RequestAcceptEncoding());
		builder.addInterceptorFirst(new ResponseContentEncoding());
		builder.setConnectionManager(cm);
		builder.setUserAgent("AerseUploader (1.17)");
		builder.setDefaultRequestConfig(requestConfig);
		return builder.build();
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
}

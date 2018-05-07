package com.aerse.uploader;

import java.io.File;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.RequestAcceptEncoding;
import org.apache.http.client.protocol.ResponseContentEncoding;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class UploaderToSelectel implements Uploader {

	private static final Logger LOG = Logger.getLogger(UploaderToSelectel.class);
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
	
	private HttpClient client;
	
	@PostConstruct
	public void start() {
		client = createClient();
	}

	@Override
	public void delete(String path) throws UploadException {
		LOG.info("deleting: " + path);
		String authToken2 = getAuthToken();
		int retryCount = 0;
		while (true) {
			HttpResponse result = null;
			try {
				HttpDelete del = new HttpDelete(baseUrl + "/" + containerName + path);
				del.addHeader("X-Auth-Token", authToken2);
				result = client.execute(del);
				if (LOG.isDebugEnabled() && result != null) {
					LOG.debug("response: " + EntityUtils.toString(result.getEntity()));
				}
				if (result != null && result.getStatusLine().getStatusCode() == HttpStatus.SC_NO_CONTENT) {
					return;
				}
				if (result != null && result.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
					throw new UploadException(UploadException.NOT_FOUND, INTERNAL_SERVER_ERROR);
				}
			} catch (UploadException e1) {
				throw e1;
			} catch (Exception e) {
				if (retryCount < retries) {
					retryCount++;
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
		LOG.info("submitting: " + path);
		String authToken2 = getAuthToken();
		int retryCount = 0;
		while (true) {
			HttpResponse result = null;
			try {
				HttpPut put = new HttpPut(baseUrl + "/" + containerName + path);
				put.addHeader("X-Auth-Token", authToken2);
				put.setEntity(new FileEntity(file));
				result = client.execute(put);
				if (LOG.isDebugEnabled()) {
					LOG.debug("response: " + EntityUtils.toString(result.getEntity()));
				}
				if (result != null && result.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
					LOG.info("invalid response: " + result.getStatusLine());
				} else {
					if (retryCount > 0) {
						LOG.info("submitted: " + path);
					}
				}
				return;
			} catch (Exception e) {
				if (retryCount < 3) {
					retryCount++;
					LOG.info("unable to submit: " + path + " retry..." + retryCount);
					try {
						Thread.sleep(1000);
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
		LOG.info("listing: " + req);
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
				LOG.info("invalid response: " + result.getStatusLine());
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
		LOG.info("downloading: " + path);
		String authToken2 = getAuthToken();
		HttpGet put = new HttpGet(baseUrl + "/" + containerName + path);
		put.addHeader("X-Auth-Token", authToken2);
		HttpResponse result = null;
		try {
			result = client.execute(put);
			if (result.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				LOG.info("unable to download: " + EntityUtils.toString(result.getEntity()));
			} else {
				f.onData(result.getEntity().getContent());
			}
		} catch (Exception e) {
			LOG.error("unable to upload", e);
		} finally {
			if (result != null) {
				EntityUtils.consumeQuietly(result.getEntity());
			}
		}
	}

	private synchronized String getAuthToken() {
		if (authToken == null || System.currentTimeMillis() > validUntil) {
			long start = System.currentTimeMillis();
			HttpGet get = new HttpGet("https://auth.selcdn.ru");
			get.addHeader("X-Auth-User", user);
			get.addHeader("X-Auth-Key", key);
			HttpResponse response = null;
			try {
				response = client.execute(get);
				if (response.getStatusLine().getStatusCode() != HttpStatus.SC_NO_CONTENT) {
					LOG.info("unable to auth: " + EntityUtils.toString(response.getEntity()));
					throw new RuntimeException("Внутренняя ошибка. Попробуйте позднее");
				}
				authToken = response.getFirstHeader("X-Auth-Token").getValue();
				baseUrl = response.getFirstHeader("X-Storage-Url").getValue();
				// convert seconds to millis
				validUntil = start + Long.valueOf(response.getFirstHeader("X-Expire-Auth-Token").getValue()) * 1000;
			} catch (Exception e) {
				throw new RuntimeException("Внутренняя ошибка. Попробуйте позднее", e);
			} finally {
				if (response != null) {
					EntityUtils.consumeQuietly(response.getEntity());
				}
			}
		}
		return authToken;
	}
	
	private HttpClient createClient() {
		return createClient(timeout);
	}

	public static HttpClient createClient(int timeout) {
		HttpParams params = new BasicHttpParams();
		HttpConnectionParams.setSoTimeout(params, timeout);
		HttpConnectionParams.setConnectionTimeout(params, timeout);
		HttpProtocolParams.setUserAgent(params, "AerseUploader");
		HttpProtocolParams.setContentCharset(params, "utf-8");

		DefaultHttpClient client = new DefaultHttpClient(new PoolingClientConnectionManager(), params);
		client.addRequestInterceptor(new RequestAcceptEncoding());
		client.addResponseInterceptor(new ResponseContentEncoding());
		return client;
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

package com.aerse.uploader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.FileEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UploaderToS3 implements Uploader {

	private static final Logger LOG = LoggerFactory.getLogger(UploaderToS3.class);
	private static final TimeZone GMT = TimeZone.getTimeZone("GMT");

	private String bucket;
	private int retries;
	private String secretKey;
	private String accessKey;
	private String region;

	private int timeout;
	private long retryTimeoutMillis;

	private HttpClient client;

	public void start() {
		client = UploaderToSelectel.createClient(timeout);
	}

	@Override
	public void submit(File file, String path) throws UploadException {
		LOG.info("submitting: {}", path);

		submitWithRetries(file, path, StorageClass.STANDARD);
	}

	@Override
	public void delete(String path) throws UploadException {
		LOG.info("deleting: {}", path);

		delete(Collections.singleton(path));
	}

	public void delete(Set<String> paths) throws UploadException {
		if (paths.isEmpty()) {
			return;
		}
		StringBuilder contentStr = new StringBuilder();
		contentStr.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete><Quiet>true</Quiet>");
		for (String cur : paths) {
			if (cur.charAt(0) == '/') {
				cur = cur.substring(1);
			}
			contentStr.append("<Object><Key>").append(cur).append("</Key></Object>");
		}
		contentStr.append("</Delete>");
		byte[] content;
		try {
			content = contentStr.toString().getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new UploadException(UploadException.INTERNAL_SERVER_ERROR, "Внутренняя ошибка. Попробуйте позднее", e);
		}
		byte[] md5 = DigestUtils.md5(content);
		String md5Base64 = new String(Base64Coder.encode(md5));

		SimpleDateFormat sdfTime = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
		sdfTime.setTimeZone(GMT);
		String date = sdfTime.format(new Date());

		HttpPost del = new HttpPost("https://" + bucket + ".s3.amazonaws.com/?delete");
		del.addHeader("Content-Type", "application/xml");
		del.addHeader("Content-MD5", md5Base64);
		del.addHeader("Date", date);
		del.addHeader("Host", bucket + ".s3.amazonaws.com");

		String canonicalString = "POST\n" + md5Base64 + "\napplication/xml\n" + date + "\n/" + bucket + "/?delete";

		try {
			del.addHeader("Authorization", "AWS " + accessKey + ":" + new String(Base64Coder.encode(signSha1(canonicalString.getBytes("UTF-8"), secretKey.getBytes("UTF-8")))));
		} catch (Exception e) {
			throw new UploadException(UploadException.INTERNAL_SERVER_ERROR, "Внутренняя ошибка. Попробуйте позднее", e);
		}
		del.setEntity(new ByteArrayEntity(content));
		HttpResponse response = null;
		try {
			response = client.execute(del);
			int code = response.getStatusLine().getStatusCode();
			if (code != HttpStatus.SC_OK && code != HttpStatus.SC_NO_CONTENT) {
				LOG.info("invalid response: {} body: {}", response, EntityUtils.toString(response.getEntity()));
				throw new UploadException(UploadException.INTERNAL_SERVER_ERROR, "Внутренняя ошибка. Попробуйте позднее");
			}
		} catch (IOException e) {
			LOG.info("unable to delete", e);
			throw new UploadException(UploadException.INTERNAL_SERVER_ERROR, "Внутренняя ошибка. Попробуйте позднее", e);
		} finally {
			if (response != null) {
				EntityUtils.consumeQuietly(response.getEntity());
			}
		}
	}

	private void submitWithRetries(File f, String path, StorageClass sc) throws UploadException {
		UploadException result = null;
		for (int i = 0; i < retries; i++) {
			try {
				submit(f, path, sc);
				break;
			} catch (UploadException e) {
				if (i < retries) {
					LOG.info("upload failed. continue...{}", i);
				}
				try {
					Thread.sleep(retryTimeoutMillis);
				} catch (InterruptedException e1) {
					LOG.info("sleep interrupted. exit");
					break;
				}
				result = e;
			}
		}
		if (result != null) {
			throw result;
		}
	}

	private String submit(File f, String path, StorageClass sc) throws UploadException {
		if (path.charAt(0) != '/') {
			path = "/" + path;
		}
		HttpPut put = new HttpPut("https://" + bucket + ".s3-" + region + ".amazonaws.com" + path);
		Date currentDate = new Date();
		HttpResponse response = null;
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
			dateFormat.setTimeZone(GMT);
			String contentHash = getContentHash(f);
			String timeFormatted = dateFormat.format(currentDate);
			String mimeType = guessMimeType(path);
			if (mimeType != null) {
				put.addHeader("Content-Type", mimeType);
			}
			put.addHeader("Host", bucket + ".s3-" + region + ".amazonaws.com");
			put.addHeader("X-Amz-Date", timeFormatted);
			put.addHeader("x-amz-content-sha256", contentHash);
			put.addHeader("x-amz-acl", "public-read");
			put.addHeader("x-amz-storage-class", sc.name());
			put.addHeader("Authorization", getAuthString(contentHash, put, currentDate));
			put.setEntity(new FileEntity(f));
			response = client.execute(put);
			if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				LOG.info("unable to submit to s3 response: " + response + EntityUtils.toString(response.getEntity()));
				throw new UploadException(UploadException.INTERNAL_SERVER_ERROR, "Внутренняя ошибка. Попробуйте позднее");
			}
			return null;
		} catch (Exception e) {
			String message = "unable to submit. f: " + f + " path: " + path;
			if (LOG.isDebugEnabled()) {
				LOG.debug(message, e);
			} else {
				LOG.info(message + " message: " + e.getMessage());
			}
			throw new UploadException(UploadException.INTERNAL_SERVER_ERROR, message, e);
		} finally {
			if (response != null) {
				EntityUtils.consumeQuietly(response.getEntity());
			}
		}
	}

	@Override
	public void download(String path, Callback f) {
		LOG.info("downloading: {}", path);

		if (path.charAt(0) != '/') {
			path = "/" + path;
		}
		HttpGet get = new HttpGet("https://" + bucket + ".s3-" + region + ".amazonaws.com" + path);
		HttpResponse response = null;
		try {
			response = client.execute(get);
			if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				LOG.info("invalid response code: {}", response);
				return;
			}
			f.onData(response.getEntity().getContent());
		} catch (Exception e) {
			LOG.error("unable to saveTo", e);
		} finally {
			if (response != null) {
				EntityUtils.consumeQuietly(response.getEntity());
			}
		}
	}

	@Override
	public List<FileEntry> listFiles(ListRequest req) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	private String getAuthString(String contentHash, HttpRequestBase method, Date currentDate) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		sdf.setTimeZone(GMT);
		String dateFormatted = sdf.format(currentDate);

		SimpleDateFormat sdfTime = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
		sdfTime.setTimeZone(GMT);

		String scope = dateFormatted + "/" + region + "/s3/aws4_request";
		String canonicalRequest = "PUT\n" + method.getURI().getPath() + "\n\nhost:" + bucket + ".s3-" + region + ".amazonaws.com\nx-amz-acl:" + method.getHeaders("x-amz-acl")[0].getValue() + "\nx-amz-date:" + method.getHeaders("x-amz-date")[0].getValue() + "\nx-amz-storage-class:" + method.getHeaders("x-amz-storage-class")[0].getValue() + "\n\nhost;x-amz-acl;x-amz-date;x-amz-storage-class\n" + contentHash;
		String stringToSign = "AWS4-HMAC-SHA256\n" + sdfTime.format(currentDate) + "\n" + scope + "\n" + toHex(DigestUtils.sha256(canonicalRequest));

		byte[] kSecret = ("AWS4" + secretKey).getBytes("UTF-8");
		byte[] kDate = sign(dateFormatted, kSecret);
		byte[] kRegion = sign(region, kDate);
		byte[] kService = sign("s3", kRegion);
		byte[] kSigning = sign("aws4_request", kService);
		byte[] signature = sign(stringToSign.getBytes("UTF-8"), kSigning);

		StringBuilder result = new StringBuilder();
		result.append("AWS4-HMAC-SHA256 ");
		result.append("Credential=");
		result.append(accessKey).append("/");
		result.append(dateFormatted).append("/");
		result.append(region).append("/s3/aws4_request, ");
		result.append("SignedHeaders=host;x-amz-acl;x-amz-date;x-amz-storage-class, ");
		result.append("Signature=");
		result.append(toHex(signature));
		return result.toString();
	}

	public static String toHex(byte[] data) {
		StringBuilder sb = new StringBuilder(data.length * 2);
		for (int i = 0; i < data.length; i++) {
			String hex = Integer.toHexString(data[i]);
			if (hex.length() == 1) {
				// Append leading zero.
				sb.append("0");
			} else if (hex.length() == 8) {
				// Remove ff prefix from negative numbers.
				hex = hex.substring(6);
			}
			sb.append(hex);
		}
		return sb.toString().toLowerCase(Locale.UK);
	}

	private static byte[] sign(String data, byte[] key) throws Exception {
		return sign(data.getBytes("UTF-8"), key);
	}

	private static byte[] sign(byte[] data, byte[] key) throws Exception {
		Mac mac = Mac.getInstance("HMACSHA256");
		mac.init(new SecretKeySpec(key, "HMACSHA256"));
		return mac.doFinal(data);
	}

	private static byte[] signSha1(byte[] data, byte[] key) throws Exception {
		Mac mac = Mac.getInstance("HmacSHA1");
		mac.init(new SecretKeySpec(key, "HmacSHA1"));
		return mac.doFinal(data);
	}

	private static String getContentHash(File file) throws Exception {
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(file);
			return toHex(DigestUtils.sha256(fis));
		} finally {
			if (fis != null) {
				fis.close();
			}
		}
	}

	private static String guessMimeType(String file) {
		if (file.endsWith(".png")) {
			return "image/png";
		} else if (file.endsWith(".jpg") || file.endsWith(".jpeg")) {
			return "image/jpeg";
		} else if (file.endsWith(".svg")) {
			return "image/svg+xml";
		} else if (file.endsWith(".pdf")) {
			return "application/pdf";
		} else if (file.endsWith(".gif")) {
			return "image/gif";
		} else {
			return null;
		}
	}

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public void setRetries(int retries) {
		this.retries = retries;
		if (this.retries == 0) {
			this.retries++;
		}
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public void setRetryTimeoutMillis(long retryTimeoutMillis) {
		this.retryTimeoutMillis = retryTimeoutMillis;
	}
}

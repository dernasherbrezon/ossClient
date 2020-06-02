package ru.r2cloud.ossclient;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class SelectelOssClientTest {

	private static final String AUTH_ENDPOINT = "/auth/v1.0";
	private static final String HOST = "localhost";
	private static final int PORT = 8000;
	private static final String CONTAINER_NAME = "container";
	private static final String BASEDATAPATH = "/data";

	private SelectelOssClient client;
	private HttpServer server;
	private FileOssClient fileClient;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testDeleteWithAuthFailure() throws Exception {
		String path = "/testFile";
		fileClient.submit(createTempFile(UUID.randomUUID().toString()), path);
		List<HttpHandler> handlers = new ArrayList<>();
		handlers.add(new DeleteHandler(fileClient, path, 401));
		handlers.add(new DeleteHandler(fileClient, path, 201));
		server.createContext(BASEDATAPATH + "/" + CONTAINER_NAME + path, new SequentialHttpHandler(handlers));
		client.delete(path);
	}

	@Test
	public void testDelete() throws Exception {
		String path = "/testFile";
		fileClient.submit(createTempFile(UUID.randomUUID().toString()), path);
		server.createContext(BASEDATAPATH + "/" + CONTAINER_NAME + path, new DeleteHandler(fileClient, path, 201));
		client.delete(path);
	}
	
	@Test(expected = OssException.class)
	public void testDeleteFailure() throws Exception {
		String path = "/testFile";
		fileClient.submit(createTempFile(UUID.randomUUID().toString()), path);
		server.createContext(BASEDATAPATH + "/" + CONTAINER_NAME + path, new DeleteHandler(fileClient, path, 503));
		client.delete(path);
	}

	@Test(expected = OssException.class)
	public void testInvalidAuth() throws Exception {
		setupContext(AUTH_ENDPOINT, new AuthHttpHandler(HOST, PORT, BASEDATAPATH, 401));
		client.delete(UUID.randomUUID().toString());
	}

	@Test(expected = OssException.class)
	public void testInvalidAuthUrl() throws Exception {
		// this will trigger 404
		server.removeContext(AUTH_ENDPOINT);
		client.delete(UUID.randomUUID().toString());
	}

	@Before
	public void start() throws Exception {
		server = HttpServer.create(new InetSocketAddress(HOST, PORT), 0);
		server.start();
		server.createContext(AUTH_ENDPOINT, new AuthHttpHandler(HOST, PORT, "/data", 204));

		client = new SelectelOssClient();
		client.setAuthUrl("http://" + HOST + ":" + PORT + AUTH_ENDPOINT);
		client.setContainerName(CONTAINER_NAME);
		client.setKey(UUID.randomUUID().toString());
		client.setRetries(3);
		client.setRetryTimeoutMillis(10_000L);
		client.setTimeout(10_000);
		client.setUser(UUID.randomUUID().toString());
		client.start();

		fileClient = new FileOssClient();
		fileClient.setBasePath(tempFolder.getRoot().getAbsolutePath());
		fileClient.start();
	}

	private void setupContext(String name, HttpHandler handler) {
		server.removeContext(name);
		server.createContext(name, handler);
	}

	@After
	public void stop() {
		if (server != null) {
			server.stop(0);
		}
	}

	private File createTempFile(String data) throws IOException {
		File tempFile = new File(tempFolder.getRoot(), UUID.randomUUID().toString());
		try (FileWriter fw = new FileWriter(tempFile)) {
			fw.append(data);
		}
		return tempFile;
	}

}

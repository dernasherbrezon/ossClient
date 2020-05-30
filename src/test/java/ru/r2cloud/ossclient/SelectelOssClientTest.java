package ru.r2cloud.ossclient;

import java.net.InetSocketAddress;
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

	private SelectelOssClient client;
	private HttpServer server;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test(expected = OssException.class)
	public void testInvalidAuth() throws Exception {
		setupContext(AUTH_ENDPOINT, new AuthHttpHandler(HOST, PORT, "/data", 401));
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
		client.setContainerName(UUID.randomUUID().toString());
		client.setKey(UUID.randomUUID().toString());
		client.setRetries(3);
		client.setRetryTimeoutMillis(10_000L);
		client.setTimeout(10_000);
		client.setUser(UUID.randomUUID().toString());
		client.start();
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
}

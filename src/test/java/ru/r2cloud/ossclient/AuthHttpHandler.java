package ru.r2cloud.ossclient;

import java.io.IOException;
import java.util.UUID;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class AuthHttpHandler implements HttpHandler {

	private final String host;
	private final int port;
	private final int statusCode;
	private final String dataBasePath;

	public AuthHttpHandler(String host, int port, String dataBasePath, int statusCode) {
		this.host = host;
		this.port = port;
		this.dataBasePath = dataBasePath;
		this.statusCode = statusCode;
	}

	@Override
	public void handle(HttpExchange exchange) throws IOException {
		exchange.getResponseHeaders().add("X-Auth-Token", UUID.randomUUID().toString());
		exchange.getResponseHeaders().add("X-Storage-Url", "http://" + host + ":" + port + dataBasePath);
		exchange.getResponseHeaders().add("X-Expire-Auth-Token", "100000");
		exchange.sendResponseHeaders(statusCode, -1);
		exchange.close();
	}
}

package ru.r2cloud.ossclient;

import java.io.IOException;
import java.util.List;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class SequentialHttpHandler implements HttpHandler {

	private final List<HttpHandler> responses;

	private int currentResponse = 0;

	public SequentialHttpHandler(List<HttpHandler> responses) {
		this.responses = responses;
	}

	@Override
	public void handle(HttpExchange exchange) throws IOException {
		if (currentResponse >= responses.size()) {
			throw new IOException();
		}
		HttpHandler response = responses.get(currentResponse);
		currentResponse++;
		response.handle(exchange);
	}
}

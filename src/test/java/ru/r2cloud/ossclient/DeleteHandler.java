package ru.r2cloud.ossclient;

import java.io.IOException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class DeleteHandler implements HttpHandler {

	private FileOssClient client;
	private String path;
	private int statusCode;

	public DeleteHandler(FileOssClient client, String path, int statusCode) {
		this.client = client;
		this.path = path;
		this.statusCode = statusCode;
	}

	@Override
	public void handle(HttpExchange exchange) throws IOException {
		int statusCodeToReply;
		if (statusCode == 201) {
			try {
				client.delete(path);
				statusCodeToReply = statusCode;
			} catch (OssException e) {
				statusCodeToReply = 400;
			}
		} else {
			statusCodeToReply = statusCode;
		}
		exchange.sendResponseHeaders(statusCodeToReply, -1);
		exchange.close();
	}

}

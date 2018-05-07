package com.aerse.uploader;

public class UploadException extends Exception {

	private static final long serialVersionUID = -2894157707774693665L;

	public static final int NOT_FOUND = 404;
	public static final int INTERNAL_SERVER_ERROR = 503;

	private int code;

	public UploadException(String message) {
		super(message);
	}

	public UploadException(int code, String message, Throwable e) {
		super(message, e);
		this.code = code;
	}

	public UploadException(int code, String message) {
		super(message);
		this.code = code;
	}

	public int getCode() {
		return code;
	}

}

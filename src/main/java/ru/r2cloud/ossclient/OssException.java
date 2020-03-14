package ru.r2cloud.ossclient;

public class OssException extends Exception {

	private static final long serialVersionUID = -2894157707774693665L;

	public static final int NOT_FOUND = 404;
	public static final int INTERNAL_SERVER_ERROR = 503;

	private int code;

	public OssException(String message) {
		super(message);
	}

	public OssException(int code, String message, Throwable e) {
		super(message, e);
		this.code = code;
	}

	public OssException(int code, String message) {
		super(message);
		this.code = code;
	}

	public int getCode() {
		return code;
	}

}

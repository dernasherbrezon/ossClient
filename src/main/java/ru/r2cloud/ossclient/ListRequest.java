package ru.r2cloud.ossclient;

public class ListRequest {

	private int limit = 10000;
	private String marker;
	private String prefix;
	private String path;
	private Character delimiter;

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public String getMarker() {
		return marker;
	}
	
	public void setMarker(String marker) {
		this.marker = marker;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public Character getDelimiter() {
		return delimiter;
	}
	
	public void setDelimiter(Character delimiter) {
		this.delimiter = delimiter;
	}

	@Override
	public String toString() {
		return "ListRequest [limit=" + limit + ", marker=" + marker + ", prefix=" + prefix + ", path=" + path + ", delimiter=" + delimiter + "]";
	}

}

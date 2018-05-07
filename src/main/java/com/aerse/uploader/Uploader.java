package com.aerse.uploader;

import java.io.File;
import java.util.List;

public interface Uploader {

	void submit(File file, String path) throws UploadException;

	void delete(String path) throws UploadException;
	
	void download(String path, Callback f);
	
	List<FileEntry> listFiles(ListRequest req);
}

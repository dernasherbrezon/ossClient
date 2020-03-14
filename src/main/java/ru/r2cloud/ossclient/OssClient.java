package ru.r2cloud.ossclient;

import java.io.File;
import java.util.List;

public interface OssClient {

	void submit(File file, String path) throws OssException;

	void delete(String path) throws OssException;

	void download(String path, Callback f) throws OssException;

	List<FileEntry> listFiles(ListRequest req) throws OssException;
}

package com.aerse.uploader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UploaderToFile implements Uploader {

	private static final Logger LOG = LoggerFactory.getLogger(UploaderToFile.class);

	private String basePath;
	private File basePathDir;

	public void start() {
		basePathDir = initDir(basePath);
	}

	@Override
	public List<FileEntry> listFiles(final ListRequest req) {
		LOG.info("listing: {}", req);

		Collection<File> it = FileUtils.listFiles(new File(basePath), new IOFileFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return accept(new File(dir, name));
			}

			@Override
			public boolean accept(File file) {
				if (req.getPrefix() == null) {
					return true;
				}
				return file.getAbsolutePath().startsWith(basePathDir.getAbsolutePath() + req.getPrefix());
			}
		}, TrueFileFilter.INSTANCE);
		boolean foundMarker = false;

		File markerFile;
		if (req.getMarker() != null) {
			markerFile = new File(basePathDir.getAbsolutePath() + req.getMarker());
		} else {
			markerFile = null;
		}

		List<File> sorted = new ArrayList<>(it);
		Collections.sort(sorted);

		List<FileEntry> result = new ArrayList<FileEntry>();
		for (File cur : sorted) {
			if (markerFile != null && !foundMarker) {
				if (cur.getAbsolutePath().equals(markerFile.getAbsolutePath())) {
					foundMarker = true;
				}
				continue;
			}

			result.add(convert(cur));
			if (result.size() >= req.getLimit() || result.size() >= 10000) {
				break;
			}
		}
		return result;
	}

	private FileEntry convert(File cur) {
		String fileName = cur.getAbsolutePath();
		String basePathName = basePathDir.getAbsolutePath();

		FileEntry result = new FileEntry();
		result.setBytes(cur.length());
		result.setName(fileName.substring(basePathName.length()));
		result.setLast_modified(new Date(cur.lastModified()).toString());
		return result;
	}

	@Override
	public void delete(String path) throws UploadException {
		LOG.info("deleting: {}", path);

		File newPath = new File(basePath + path);
		if (!newPath.exists()) {
			return;
		}
		if (newPath.isDirectory()) {
			try {
				FileUtils.deleteDirectory(newPath);
			} catch (IOException e) {
				throw new UploadException(UploadException.INTERNAL_SERVER_ERROR, "unable to delete directory: " + newPath.getAbsolutePath(), e);
			}

		} else if (newPath.isFile()) {
			if (!newPath.delete()) {
				throw new UploadException(UploadException.INTERNAL_SERVER_ERROR, "unable to delete file: " + newPath.getAbsolutePath());
			}
		}
	}

	@Override
	public void submit(File file, String path) throws UploadException {
		LOG.info("submitting: {}", path);

		File newPath = new File(basePath + path);
		if (!newPath.getParentFile().exists() && !newPath.getParentFile().mkdirs()) {
			throw new UploadException("Unable to create dirs: " + newPath.getParentFile().getAbsolutePath());
		}
		FileInputStream fis = null;
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(newPath);
			fis = new FileInputStream(file);
			IOUtils.copy(fis, fos);
		} catch (IOException e) {
			throw new UploadException(UploadException.INTERNAL_SERVER_ERROR, "unable to copy", e);
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					LOG.info("unable to close cursors", e);
				}
			}
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					LOG.info("unable to close cursors", e);
				}
			}
		}
	}

	@Override
	public void download(String path, Callback f) {
		LOG.info("downloading: {}", path);

		File filePath = new File(basePath + path);
		if (filePath.exists()) {
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(filePath);
				f.onData(fis);
			} catch (Exception e) {
				LOG.error("unable to callback", e);
			} finally {
				IOUtils.closeQuietly(fis);
			}
		}
	}

	private static File initDir(String dir) {
		File tempDirFile = new File(dir);
		if (tempDirFile.exists() && !tempDirFile.isDirectory()) {
			throw new IllegalStateException("is not a directory: " + tempDirFile.getAbsolutePath());
		}
		if (!tempDirFile.exists() && !tempDirFile.mkdirs()) {
			throw new IllegalStateException("unable to create: " + tempDirFile.getAbsolutePath());
		}
		return tempDirFile;
	}

	public void setBasePath(String basePath) {
		this.basePath = basePath;
	}
}

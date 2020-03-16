package ru.r2cloud.ossclient;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileOssClient implements OssClient {

	private static final Logger LOG = LoggerFactory.getLogger(FileOssClient.class);

	private String basePath;
	private File basePathDir;

	public void start() {
		basePathDir = initDir(basePath);
	}

	@Override
	public List<FileEntry> listFiles(final ListRequest req) throws OssException {
		LOG.info("listing: {}", req);

		List<File> sorted = new ArrayList<>();
		try {
			Files.walkFileTree(basePathDir.toPath(), new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					if (req.getPrefix() == null || file.toFile().getAbsolutePath().startsWith(basePathDir.getAbsolutePath() + req.getPrefix())) {
						sorted.add(file.toFile());
					}
					return FileVisitResult.CONTINUE;
				}
			});
		} catch (IOException e) {
			throw new OssException(OssException.INTERNAL_SERVER_ERROR, "unable to list", e);
		}
		boolean foundMarker = false;

		File markerFile;
		if (req.getMarker() != null) {
			markerFile = new File(basePathDir.getAbsolutePath() + req.getMarker());
		} else {
			markerFile = null;
		}

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
		result.setLastModified(new Date(cur.lastModified()).toString());
		return result;
	}

	@Override
	public void delete(String path) throws OssException {
		LOG.info("deleting: {}", path);

		File newPath = new File(basePath + path);
		if (!newPath.exists()) {
			throw new OssException(404, "path not found");
		}
		try {
			Files.walkFileTree(newPath.toPath(), new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					Files.delete(dir);
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					Files.delete(file);
					return FileVisitResult.CONTINUE;
				}
			});
		} catch (IOException e1) {
			throw new OssException(OssException.INTERNAL_SERVER_ERROR, "unable to delete path", e1);
		}
	}

	@Override
	public void submit(File file, String path) throws OssException {
		LOG.info("submitting: {}", path);

		File newPath = new File(basePath + path);
		if (!newPath.getParentFile().exists() && !newPath.getParentFile().mkdirs()) {
			throw new OssException("Unable to create dirs: " + newPath.getParentFile().getAbsolutePath());
		}
		try (FileOutputStream fos = new FileOutputStream(newPath)) {
			Files.copy(file.toPath(), fos);
		} catch (IOException e) {
			throw new OssException(OssException.INTERNAL_SERVER_ERROR, "unable to copy", e);
		}
	}

	@Override
	public void download(String path, Callback f) {
		LOG.info("downloading: {}", path);

		File filePath = new File(basePath + path);
		if (filePath.exists()) {
			try (FileInputStream fis = new FileInputStream(filePath)) {
				f.onData(fis);
			} catch (Exception e) {
				LOG.error("unable to callback", e);
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

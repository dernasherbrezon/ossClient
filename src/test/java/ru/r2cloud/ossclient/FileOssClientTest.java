package ru.r2cloud.ossclient;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileOssClientTest {

	private FileOssClient fileClient;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testDownload() throws Exception {
		fileClient = new FileOssClient();
		fileClient.setBasePath(tempFolder.getRoot().getAbsolutePath());
		fileClient.start();

		String data = UUID.randomUUID().toString();
		File tempFile = createTempFile(data);
		String path = "/v1/" + UUID.randomUUID().toString() + "/" + tempFile.getName();
		fileClient.submit(tempFile, path);

		fileClient.download(path, new Callback() {

			@Override
			public void onData(InputStream is) {
				try (BufferedReader r = new BufferedReader(new InputStreamReader(is))) {
					assertEquals(data, r.readLine());
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
	}

	@Test
	public void testSubmit() throws Exception {
		fileClient = new FileOssClient();
		fileClient.setBasePath(tempFolder.getRoot().getAbsolutePath());
		fileClient.start();

		File tempFile = createTempFile(UUID.randomUUID().toString());
		fileClient.submit(tempFile, "/v1/" + UUID.randomUUID().toString() + "/" + tempFile.getName());
	}

	private File createTempFile(String data) throws IOException {
		File tempFile = new File(tempFolder.getRoot(), UUID.randomUUID().toString());
		try (FileWriter fw = new FileWriter(tempFile)) {
			fw.append(data);
		}
		return tempFile;
	}

	@Test(expected = OssException.class)
	public void testDeleteUnknown() throws Exception {
		fileClient.delete(UUID.randomUUID().toString());
	}

	@Test
	public void testDelete() throws Exception {
		fileClient = new FileOssClient();
		fileClient.setBasePath(tempFolder.getRoot().getAbsolutePath());
		fileClient.start();

		File tempFile = createTempFile(UUID.randomUUID().toString());
		String path = "/v1/" + UUID.randomUUID().toString() + "/" + tempFile.getName();
		fileClient.submit(tempFile, path);
		fileClient.delete(path);
	}

	@Test
	public void testList4() throws Exception {
		ListRequest req = new ListRequest();
		req.setLimit(1);
		List<FileEntry> result = fileClient.listFiles(req);
		assertEquals(1, result.size());
		assertEquals("/a1/b1/1.txt", result.get(0).getName());
		req.setMarker("/a1/b1/1.txt");
		result = fileClient.listFiles(req);
		assertEquals(1, result.size());
		assertEquals("/a1/b2/2.txt", result.get(0).getName());
		req.setMarker("/a1/b2/2.txt");
		result = fileClient.listFiles(req);
		assertEquals(1, result.size());
		assertEquals("/a2/3.txt", result.get(0).getName());
	}

	@Test
	public void testList3() throws Exception {
		ListRequest req = new ListRequest();
		req.setLimit(1);
		List<FileEntry> result = fileClient.listFiles(req);
		assertEquals(1, result.size());
	}

	@Test
	public void testList2() throws Exception {
		ListRequest req = new ListRequest();
		req.setPrefix("/a1/b");
		List<FileEntry> result = fileClient.listFiles(req);
		assertEquals(2, result.size());
	}

	@Test
	public void testList() throws Exception {
		ListRequest req = new ListRequest();
		List<FileEntry> result = fileClient.listFiles(req);
		assertEquals(3, result.size());
	}

	@Before
	public void start() {
		fileClient = new FileOssClient();
		fileClient.setBasePath("./src/test/resources/listtest");
		fileClient.start();
	}

}

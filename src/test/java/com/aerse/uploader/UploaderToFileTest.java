package com.aerse.uploader;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class UploaderToFileTest {

	private UploaderToFile uploader;
	
	@Test
	public void testList4() {
		ListRequest req = new ListRequest();
		req.setLimit(1);
		List<FileEntry> result = uploader.listFiles(req);
		assertEquals(1, result.size());
		assertEquals("/a1/b1/1.txt", result.get(0).getName());
		req.setMarker("/a1/b1/1.txt");
		result = uploader.listFiles(req);
		assertEquals(1, result.size());
		assertEquals("/a1/b2/2.txt", result.get(0).getName());
		req.setMarker("/a1/b2/2.txt");
		result = uploader.listFiles(req);
		assertEquals(1, result.size());
		assertEquals("/a2/3.txt", result.get(0).getName());
	}

	@Test
	public void testList3() {
		ListRequest req = new ListRequest();
		req.setLimit(1);
		List<FileEntry> result = uploader.listFiles(req);
		assertEquals(1, result.size());
	}
	
	@Test
	public void testList2() {
		ListRequest req = new ListRequest();
		req.setPrefix("/a1/b");
		List<FileEntry> result = uploader.listFiles(req);
		assertEquals(2, result.size());
	}

	@Test
	public void testList() {
		ListRequest req = new ListRequest();
		List<FileEntry> result = uploader.listFiles(req);
		assertEquals(3, result.size());
	}

	@Before
	public void start() {
		uploader = new UploaderToFile();
		uploader.setBasePath("./src/test/resources/listtest");
		uploader.start();
	}

}

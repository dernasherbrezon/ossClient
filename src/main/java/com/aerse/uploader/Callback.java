package com.aerse.uploader;

import java.io.InputStream;

public interface Callback {

	void onData(InputStream is);
	
}

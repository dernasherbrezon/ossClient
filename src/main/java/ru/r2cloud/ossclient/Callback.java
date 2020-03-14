package ru.r2cloud.ossclient;

import java.io.InputStream;

public interface Callback {

	void onData(InputStream is);
	
}

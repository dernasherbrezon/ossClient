package ru.r2cloud.ossclient;

import java.io.IOException;

interface RetryFunction {

	boolean apply(int currentRetry) throws IOException, InterruptedException, OssException;
	
}

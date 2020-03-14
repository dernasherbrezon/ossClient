## About [![Build Status](https://travis-ci.org/dernasherbrezon/ossClient.svg?branch=master)](https://travis-ci.org/dernasherbrezon/ossClient) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=ru.r2cloud%3AossClient&metric=alert_status)](https://sonarcloud.io/dashboard?id=ru.r2cloud%3AossClient)
 
Single interface for objects storage services. Currently supported:

  * [Selectel](https://selectel.ru/services/cloud/storage/)
  * File system. Used mostly in dev
  
## Features

  * Native support for JDK11. Native httpclient and JDK11 features
  * Lightweight. Depends only on minimal-json and slf4j-api
  * Retry requests
  
## Usage

1. Add maven dependency:

```xml
<dependency>
	<groupId>ru.r2cloud</groupId>
	<artifactId>ossClient</artifactId>
	<version>2.0</version>
</dependency>
```

2. Instantiate OssClient:

```java
SelectelOssClient client = new SelectelOssClient();
client.authUrl = "https://api.selcdn.ru/auth/v1.0";
client.containerName = "container";
client.retries = 3;
client.retryTimeoutMillis = 10000;
client.timeout = 10000;
client.user = "user";
client.key = "password";
client.start();
```

3. Upload:

```java
client.submit(file, "/v1/subfolder/file.jpg");
```

## Implementation notes

  * Not all Openstack swift methods supported

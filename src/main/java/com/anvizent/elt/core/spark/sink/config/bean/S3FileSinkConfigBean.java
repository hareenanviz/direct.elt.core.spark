package com.anvizent.elt.core.spark.sink.config.bean;

import com.anvizent.elt.core.spark.config.bean.S3Connection;

/**
 * @author Hareen Bejjanki
 *
 */
public class S3FileSinkConfigBean extends FileSinkConfigBean implements SinkConfigBean {

	private static final long serialVersionUID = 1L;

	private S3Connection s3Connection;

	public S3Connection getS3Connection() {
		return s3Connection;
	}

	public void setS3Connection(S3Connection s3Connection) {
		this.s3Connection = s3Connection;
	}

	public void setS3Connection(String accessKey, String secretKey, String bucketName) {
		if (this.s3Connection == null) {
			this.s3Connection = new S3Connection();
		}
		this.s3Connection.setAccessKey(accessKey);
		this.s3Connection.setSecretKey(secretKey);
		this.s3Connection.setBucketName(bucketName);
	}
}

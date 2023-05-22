package com.anvizent.elt.core.spark.config.bean;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class S3Connection extends ConfigBean {
	private static final long serialVersionUID = 1L;

	private String accessKey;
	private String secretKey;
	private String bucketName;

	public String getAccessKey() {
		return accessKey;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}

	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}
}

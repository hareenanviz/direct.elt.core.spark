package com.anvizent.elt.core.spark.source.config.bean;

import com.anvizent.elt.core.spark.config.bean.S3Connection;
import com.anvizent.elt.core.spark.constant.Constants;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SourceS3CSVFileConfigBean extends SourceCSVFileConfigBean implements SourceConfigBean {
	private static final long serialVersionUID = 1L;

	private S3Connection s3Connection;
	private String rawPath;

	@Override
	public void setPath(String path) {
		setPath(path, true);
	}

	public void setPath(String path, boolean setRaw) {
		super.setPath(path);
		if (setRaw) {
			this.rawPath = path;
		}
	}

	@Override
	public void addPrefix(String prefix) {
		if (prefix != null && !prefix.isEmpty()) {
			super.setPath(Constants.FILE_PATH_SEPARATOR + prefix + getPath());
		}

		this.rawPath = getPath();
	}

	public String getRawPath() {
		return rawPath;
	}

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
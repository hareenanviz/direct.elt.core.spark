package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum Compression {
	NONE("none"), BZIP2("bzip2"), GZIP("gzip"), LZ4("lz4"), SNAPPY("snappy"), DEFLATE("deflate");

	private String value;

	public String getValue() {
		return value;
	}

	private Compression(String value) {
		this.value = value;
	}

	public static Compression getInstance(String value) {
		return getInstance(value, null);
	}

	public static Compression getInstance(String value, Compression defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(NONE.value)) {
			return NONE;
		} else if (value.equalsIgnoreCase(BZIP2.value)) {
			return BZIP2;
		} else if (value.equalsIgnoreCase(GZIP.value)) {
			return GZIP;
		} else if (value.equalsIgnoreCase(LZ4.value)) {
			return LZ4;
		} else if (value.equalsIgnoreCase(SNAPPY.value)) {
			return SNAPPY;
		} else if (value.equalsIgnoreCase(DEFLATE.value)) {
			return DEFLATE;
		} else {
			return null;
		}
	}
}

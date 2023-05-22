package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum KafkaSinkCompression {
	NONE("none"), GZIP("gzip"), LZ4("lz4"), SNAPPY("snappy");

	private String value;

	public String getValue() {
		return value;
	}

	private KafkaSinkCompression(String value) {
		this.value = value;
	}

	public static KafkaSinkCompression getInstance(String value) {
		return getInstance(value, null);
	}

	public static KafkaSinkCompression getInstance(String value, KafkaSinkCompression defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(NONE.value)) {
			return NONE;
		} else if (value.equalsIgnoreCase(GZIP.value)) {
			return GZIP;
		} else if (value.equalsIgnoreCase(LZ4.value)) {
			return LZ4;
		} else if (value.equalsIgnoreCase(SNAPPY.value)) {
			return SNAPPY;
		} else {
			return null;
		}
	}
}

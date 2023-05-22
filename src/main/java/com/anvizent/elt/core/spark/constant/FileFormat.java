package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 *
 */
public enum FileFormat {
	CSV("com.databricks.spark.csv"), JSON("json");

	private String value;

	public String getValue() {
		return value;
	}

	private FileFormat(String value) {
		this.value = value;
	}

	public static FileFormat getInstance(String value) {
		return getInstance(value, JSON);
	}

	public static FileFormat getInstance(String value, FileFormat defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(CSV.value)) {
			return CSV;
		} else if (value.equalsIgnoreCase(JSON.value)) {
			return JSON;
		} else {
			return null;
		}
	}
}

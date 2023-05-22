package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum DBWriteMode {

	APPEND("APPEND"), OVERWRITE("OVERWRITE"), TRUNCATE("TRUNCATE"), IGNORE("IGNORE"), FAIL("FAIL");

	private String value;

	private DBWriteMode(String value) {
		this.value = value;
	}

	public static DBWriteMode getInstance(String value) {
		return getInstance(value, APPEND);
	}

	public static DBWriteMode getInstance(String value, DBWriteMode defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(APPEND.value)) {
			return APPEND;
		} else if (value.equalsIgnoreCase(OVERWRITE.value)) {
			return OVERWRITE;
		} else if (value.equalsIgnoreCase(TRUNCATE.value)) {
			return TRUNCATE;
		} else if (value.equalsIgnoreCase(IGNORE.value)) {
			return IGNORE;
		} else if (value.equalsIgnoreCase(FAIL.value)) {
			return FAIL;
		} else {
			return null;
		}
	}
}

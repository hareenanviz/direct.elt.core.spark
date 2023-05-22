package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum OnZeroFetchOperation {
		INSERT("INSERT"), IGNORE("IGNORE"), FAIL("FAIL");

	private String value;

	private OnZeroFetchOperation(String value) {
		this.value = value;
	}

	public static OnZeroFetchOperation getInstance(String value) {
		return getInstance(value, FAIL);
	}

	private static OnZeroFetchOperation getInstance(String value, OnZeroFetchOperation defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(INSERT.value)) {
			return INSERT;
		} else if (value.equalsIgnoreCase(IGNORE.value)) {
			return IGNORE;
		} else if (value.equalsIgnoreCase(FAIL.value)) {
			return FAIL;
		} else {
			return null;
		}
	}

}

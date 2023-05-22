package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum MalformedRows {

		STOP_AND_FAIL("STOP_AND_FAIL", "FAIL_FAST"), IGNORE("IGNORE", "DROP_MALFORMED");

	private String value;
	private String optionValue;

	public String getValue() {
		return value;
	}

	public String getOptionValue() {
		return optionValue;
	}

	private MalformedRows(String value, String optionValue) {
		this.value = value;
		this.optionValue = optionValue;
	}

	public static MalformedRows getInstance(String value) {
		return getInstance(value, STOP_AND_FAIL);
	}

	public static MalformedRows getInstance(String value, MalformedRows defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(STOP_AND_FAIL.value)) {
			return STOP_AND_FAIL;
		} else if (value.equalsIgnoreCase(IGNORE.value)) {
			return IGNORE;
		} else {
			return null;
		}
	}

}

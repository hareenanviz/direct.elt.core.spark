package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum KafkaSinkWriteFormat {
	JSON("json"), PLAIN_TEXT("plain_text");

	private String value;

	public String getValue() {
		return value;
	}

	private KafkaSinkWriteFormat(String value) {
		this.value = value;
	}

	public static KafkaSinkWriteFormat getInstance(String value) {
		return getInstance(value, null);
	}

	public static KafkaSinkWriteFormat getInstance(String value, KafkaSinkWriteFormat defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(JSON.value)) {
			return JSON;
		} else if (value.equalsIgnoreCase(PLAIN_TEXT.value)) {
			return PLAIN_TEXT;
		} else {
			return null;
		}
	}
}

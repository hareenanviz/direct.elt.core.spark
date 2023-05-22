package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum AppendAt {
	BEGIN("BEGIN"), BEFORE("BEFORE"), NEXT("NEXT"), END("END");

	private String value;

	private AppendAt(String value) {
		this.value = value;
	}

	public static AppendAt getInstance(String value) {
		return getInstance(value, END);
	}

	private static AppendAt getInstance(String value, AppendAt defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(END.value)) {
			return END;
		} else if (value.equalsIgnoreCase(BEGIN.value)) {
			return BEGIN;
		} else if (value.equalsIgnoreCase(BEFORE.value)) {
			return BEFORE;
		} else if (value.equalsIgnoreCase(NEXT.value)) {
			return NEXT;
		} else {
			return null;
		}
	}
}

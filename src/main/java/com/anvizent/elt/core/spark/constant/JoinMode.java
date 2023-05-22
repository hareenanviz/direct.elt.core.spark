package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 *
 */
public enum JoinMode {

	REGULAR("REGULAR"), BROADCAST_LEFT("BROADCAST_LEFT"), BROADCAST_RIGHT("BROADCAST_RIGHT"), BROADCAST_BY_ROWS("BROADCAST_BY_ROWS"),
	BROADCAST_BY_SIZE("BROADCAST_BY_SIZE"), SEMI_BROADCAST("SEMI_BROADCAST");

	private String value;

	private JoinMode(String value) {
		this.value = value;
	}

	public static JoinMode getInstance(String value) {
		return getInstance(value, JoinMode.REGULAR);
	}

	private static JoinMode getInstance(String value, JoinMode defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(REGULAR.value)) {
			return REGULAR;
		} else if (value.equalsIgnoreCase(BROADCAST_LEFT.value)) {
			return BROADCAST_LEFT;
		} else if (value.equalsIgnoreCase(BROADCAST_RIGHT.value)) {
			return BROADCAST_RIGHT;
		} else if (value.equalsIgnoreCase(BROADCAST_BY_ROWS.value)) {
			return BROADCAST_BY_ROWS;
		} else if (value.equalsIgnoreCase(BROADCAST_BY_SIZE.value)) {
			return BROADCAST_BY_SIZE;
		} else if (value.equalsIgnoreCase(SEMI_BROADCAST.value)) {
			return SEMI_BROADCAST;
		} else {
			return null;
		}
	}

}

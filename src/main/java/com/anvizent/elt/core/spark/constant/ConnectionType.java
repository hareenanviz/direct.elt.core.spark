package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum ConnectionType {

	SQL("SQL"), RETHINK("RETHINK");

	private String value;

	private ConnectionType(String value) {
		this.value = value;
	}

	public static ConnectionType getInstance(String value) {
		return getInstance(value, SQL);
	}

	private static ConnectionType getInstance(String value, ConnectionType defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(SQL.value)) {
			return SQL;
		} else if (value.equalsIgnoreCase(RETHINK.value)) {
			return RETHINK;
		} else {
			return null;
		}
	}
}

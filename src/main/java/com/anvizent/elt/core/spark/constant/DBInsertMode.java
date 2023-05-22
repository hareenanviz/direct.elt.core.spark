package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum DBInsertMode {

	INSERT("INSERT"), UPDATE("UPDATE"), UPSERT("UPSERT"), INSERT_IF_NOT_EXISTS("INSERT_IF_NOT_EXISTS");

	private String value;

	private DBInsertMode(String value) {
		this.value = value;
	}

	public static DBInsertMode getInstance(String value) {
		return getInstance(value, UPSERT);
	}

	public static DBInsertMode getInstance(String value, DBInsertMode defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(UPSERT.value)) {
			return UPSERT;
		} else if (value.equalsIgnoreCase(UPDATE.value)) {
			return UPDATE;
		} else if (value.equalsIgnoreCase(INSERT.value)) {
			return INSERT;
		} else if (value.equalsIgnoreCase(INSERT_IF_NOT_EXISTS.value)) {
			return INSERT_IF_NOT_EXISTS;
		} else {
			return null;
		}
	}
}

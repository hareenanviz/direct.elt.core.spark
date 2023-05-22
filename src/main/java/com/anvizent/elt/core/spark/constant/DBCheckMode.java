package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum DBCheckMode {

	DB_QUERY("DB_QUERY"), PREFECTH("PREFETCH"), REGULAR("REGULAR"), LOAD_ALL("LOAD_ALL");

	private String value;

	private DBCheckMode(String value) {
		this.value = value;
	}

	public static DBCheckMode getInstance(String value) {
		return getInstance(value, REGULAR);
	}

	public static DBCheckMode getInstance(String value, DBCheckMode defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(DB_QUERY.value)) {
			return DB_QUERY;
		} else if (value.equalsIgnoreCase(REGULAR.value)) {
			return REGULAR;
		} else if (value.equalsIgnoreCase(PREFECTH.value)) {
			return PREFECTH;
		} else if (value.equalsIgnoreCase(LOAD_ALL.value)) {
			return LOAD_ALL;
		} else {
			return null;
		}
	}
}

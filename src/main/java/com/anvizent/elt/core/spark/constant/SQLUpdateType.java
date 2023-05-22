package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 *
 */
public enum SQLUpdateType {
	CSV("CSV"), BATCH("BATCH"), TEMP_TABLE("TEMP_TABLE");

	private String value;

	private SQLUpdateType(String value) {
		this.value = value;
	}

	public static SQLUpdateType getInstance(String batchType) {
		return getInstance(batchType, TEMP_TABLE);
	}

	public static SQLUpdateType getInstance(String batchType, SQLUpdateType defaultValue) {
		if (batchType == null || batchType.isEmpty()) {
			return defaultValue;
		} else if (batchType.equalsIgnoreCase(CSV.value)) {
			return CSV;
		} else if (batchType.equalsIgnoreCase(BATCH.value)) {
			return BATCH;
		} else if (batchType.equalsIgnoreCase(TEMP_TABLE.value)) {
			return TEMP_TABLE;
		} else {
			return null;
		}
	}
}

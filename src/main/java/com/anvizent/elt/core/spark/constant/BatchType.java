package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum BatchType {
	ALL("ALL"), BATCH_BY_SIZE("BATCH_BY_SIZE"), NONE("NONE");

	private String value;

	private BatchType(String value) {
		this.value = value;
	}

	public static BatchType getInstance(String batchType) {
		return getInstance(batchType, NONE);
	}

	private static BatchType getInstance(String batchType, BatchType defaultValue) {
		if (batchType == null || batchType.isEmpty()) {
			return defaultValue;
		} else if (batchType.equalsIgnoreCase(ALL.value)) {
			return ALL;
		} else if (batchType.equalsIgnoreCase(BATCH_BY_SIZE.value)) {
			return BATCH_BY_SIZE;
		} else if (batchType.equalsIgnoreCase(NONE.value)) {
			return NONE;
		} else {
			return null;
		}
	}
}

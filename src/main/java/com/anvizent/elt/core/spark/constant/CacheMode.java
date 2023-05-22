package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum CacheMode {
		LOCAL("LOCAL"), CLUSTER("CLUSTER");

	private String value;

	private CacheMode(String value) {
		this.value = value;
	}

	public static CacheMode getInstance(String value) {
		return getInstance(value, null);
	}

	public static CacheMode getInstance(String value, CacheMode defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(LOCAL.value)) {
			return LOCAL;
		} else if (value.equalsIgnoreCase(CLUSTER.value)) {
			return CLUSTER;
		} else {
			return null;
		}
	}
}

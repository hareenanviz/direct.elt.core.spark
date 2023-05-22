package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum CacheType {
		
	NONE("NONE"), EHCACHE("EHCACHE"), ELASTIC_CACHE("ELASTIC_CACHE"), MEMCACHE("MEMCACHE");

	private String value;

	private CacheType(String value) {
		this.value = value;
	}

	public static CacheType getInstance(String value) {
		return getInstance(value, NONE);
	}

	public static CacheType getInstance(String value, CacheType defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(EHCACHE.value)) {
			return EHCACHE;
		} else if (value.equalsIgnoreCase(NONE.value)) {
			return NONE;
		} else if (value.equalsIgnoreCase(ELASTIC_CACHE.value)) {
			return ELASTIC_CACHE;
		} else if (value.equalsIgnoreCase(MEMCACHE.value)) {
			return MEMCACHE;
		} else {
			return null;
		}
	}
}

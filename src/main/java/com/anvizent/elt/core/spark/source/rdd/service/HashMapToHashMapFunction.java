package com.anvizent.elt.core.spark.source.rdd.service;

import java.util.HashMap;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("rawtypes")
public class HashMapToHashMapFunction implements Function<HashMap, HashMap<String, Object>> {
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	@Override
	public HashMap<String, Object> call(HashMap map) throws Exception {
		return map;
	}

}

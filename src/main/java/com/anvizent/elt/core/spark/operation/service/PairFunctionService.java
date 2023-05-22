package com.anvizent.elt.core.spark.operation.service;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class PairFunctionService {

	public static ArrayList<Object> getKeyFieldValues(ArrayList<String> keyFields, HashMap<String, Object> row) {
		ArrayList<Object> keyFieldValues = new ArrayList<>();

		for (String keyField : keyFields) {
			if (row.containsKey(keyField)) {
				keyFieldValues.add(row.get(keyField));
			}
		}

		return keyFieldValues;
	}

	public static HashMap<String, Object> getKeyFieldsAndValues(ArrayList<String> keyFields, HashMap<String, Object> row) {
		HashMap<String, Object> keyFieldsAndValues = new HashMap<String, Object>();

		for (String keyField : keyFields) {
			if (row.containsKey(keyField)) {
				keyFieldsAndValues.put(keyField, row.get(keyField));
			}
		}

		return keyFieldsAndValues;
	}
}

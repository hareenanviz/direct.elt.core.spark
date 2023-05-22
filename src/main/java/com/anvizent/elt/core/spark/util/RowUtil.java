package com.anvizent.elt.core.spark.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentDataType;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RowUtil implements Serializable {
	private static final long serialVersionUID = 1L;

	public static HashMap<String, Object> addElements(HashMap<String, Object> valuesToAdd, LinkedHashMap<String, AnvizentDataType> newStructure) {
		return addElements(new HashMap<>(), valuesToAdd, newStructure);
	}

	public static HashMap<String, Object> addElements(HashMap<String, Object> row, HashMap<String, Object> valuesToAdd,
			LinkedHashMap<String, AnvizentDataType> newStructure) {
		HashMap<String, Object> newRow = new HashMap<>();

		for (String key : newStructure.keySet()) {
			if (valuesToAdd != null && valuesToAdd.containsKey(key)) {
				newRow.put(key, valuesToAdd.get(key));
			} else {
				newRow.put(key, row.get(key));
			}
		}

		return newRow;
	}

	public static HashMap<String, Object> changeFieldsToDifferColumns(HashMap<String, Object> inputRow, ArrayList<String> keyFields,
			ArrayList<String> keyColumns, ArrayList<String> fieldsDifferToColumns, ArrayList<String> columnsDifferToFields) {
		HashMap<String, Object> row = new HashMap<>(inputRow);

		if (keyFields != null && !keyFields.isEmpty() && keyColumns != null && !keyColumns.isEmpty()) {
			for (int i = 0; i < keyFields.size(); i++) {
				if (inputRow.keySet().contains(keyFields.get(i))) {
					row.put(keyColumns.get(i), inputRow.get(keyFields.get(i)));
					if (!keyFields.get(i).equals(keyColumns.get(i))) {
						row.remove(keyFields.get(i));
					}
				}
			}
		}

		if (fieldsDifferToColumns != null && !fieldsDifferToColumns.isEmpty() && columnsDifferToFields != null && !columnsDifferToFields.isEmpty()) {
			for (int i = 0; i < fieldsDifferToColumns.size(); i++) {
				if (inputRow.keySet().contains(fieldsDifferToColumns.get(i))) {
					row.put(columnsDifferToFields.get(i), inputRow.get(fieldsDifferToColumns.get(i)));
					if (!fieldsDifferToColumns.get(i).equals(columnsDifferToFields.get(i))) {
						row.remove(fieldsDifferToColumns.get(i));
					}
				}
			}
		}

		return row;
	}

	public static HashMap<String, Object> addConstantElements(HashMap<String, Object> row, HashMap<String, Object> valuesToAdd) {
		HashMap<String, Object> newRow = new HashMap<>(row);

		for (String key : valuesToAdd.keySet()) {
			newRow.put(key, valuesToAdd.get(key));
		}

		return newRow;
	}
}
package com.anvizent.elt.core.spark.operation.service;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import com.anvizent.elt.core.lib.AnvizentDataType;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class JoinService {

	public static LinkedHashMap<String, Object> buildNewRow(LinkedHashMap<String, AnvizentDataType> structure, HashMap<String, Object> lhsRow,
			HashMap<String, Object> rhsRow, String lhsPrefix, String rhsPrefix, HashMap<String, String> lhsNewNames, HashMap<String, String> rhsNewNames) {
		LinkedHashMap<String, Object> newRow = new LinkedHashMap<String, Object>();

		for (Entry<String, AnvizentDataType> structureEntry : structure.entrySet()) {
			if (lhsNewNames.containsKey(structureEntry.getKey())) {
				newRow.put(structureEntry.getKey(), lhsRow.get(lhsNewNames.get(structureEntry.getKey())));
			} else if (rhsNewNames.containsKey(structureEntry.getKey())) {
				newRow.put(structureEntry.getKey(), rhsRow.get(rhsNewNames.get(structureEntry.getKey())));
			} else {
				newRow.put(structureEntry.getKey(), null);
			}
		}

		return newRow;
	}
}

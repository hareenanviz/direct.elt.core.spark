package com.anvizent.elt.core.spark.sink.service;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RethinkUpdateService implements Serializable {
	private static final long serialVersionUID = 1L;

	public static boolean checkForUpdate(Map<String, Object> resultRow, LinkedHashMap<String, Object> row, ArrayList<String> metaDataFields,
			ArrayList<String> keyFileds, ArrayList<String> keyColumns, ArrayList<String> fieldsDifferToColumns, ArrayList<String> columnsDifferToFields)
			throws SQLException {
		boolean doUpdate = false;

		for (String rowKey : row.keySet()) {
			if (!keyFileds.contains(rowKey)) {

			}
		}

		return doUpdate;
	}
}

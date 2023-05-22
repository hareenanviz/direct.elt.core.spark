package com.anvizent.elt.core.spark.common.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Hareen Bejjanki
 *
 */
public class QueryInfo implements Serializable {
	private static final long serialVersionUID = 1L;

	private int currentIndex;
	private String query;
	private Map<String, List<Integer>> multiValuedFieldIndexesToSet = new HashMap<>();
	private List<String> fieldsToSet = new LinkedList<>();

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public Map<String, List<Integer>> getMultiValuedFieldIndexesToSet() {
		return multiValuedFieldIndexesToSet;
	}

	public void setMultiValuedFieldIndexesToSet(Map<String, List<Integer>> multiValuedFieldsIndexesToSet) {
		this.multiValuedFieldIndexesToSet = multiValuedFieldsIndexesToSet;
	}

	public List<String> getFieldsToSet() {
		return fieldsToSet;
	}

	public void setsToSet(List<String> fieldsToSet) {
		this.fieldsToSet = fieldsToSet;
	}

	public void addFieldIndex(String field) {
		if (!multiValuedFieldIndexesToSet.containsKey(field)) {
			multiValuedFieldIndexesToSet.put(field, new ArrayList<Integer>());
		}

		multiValuedFieldIndexesToSet.get(field).add(++currentIndex);
	}

}

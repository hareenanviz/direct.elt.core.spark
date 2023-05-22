package com.anvizent.elt.core.spark.source.rdd.service;

import java.util.HashMap;
import java.util.LinkedList;

import scala.collection.AbstractIterator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings("rawtypes")
public class ListOfHashMapIterator extends AbstractIterator<HashMap> {

	private final LinkedList<HashMap<String, Object>> rows;
	private int index = 0;

	public ListOfHashMapIterator(LinkedList<HashMap<String, Object>> rows) {
		this.rows = rows;
	}

	@Override
	public boolean hasNext() {
		return index < rows.size();
	}

	@Override
	public HashMap<String, Object> next() {
		return rows.get(index++);
	}
}

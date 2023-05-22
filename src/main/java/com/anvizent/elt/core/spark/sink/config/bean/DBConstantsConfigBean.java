package com.anvizent.elt.core.spark.sink.config.bean;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class DBConstantsConfigBean implements Serializable {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> columns;
	private ArrayList<String> values;
	private ArrayList<String> types;
	private ArrayList<Integer> indexes;

	public ArrayList<String> getColumns() {
		return columns;
	}

	public void setColumns(ArrayList<String> columns) {
		this.columns = columns;
	}

	public ArrayList<String> getValues() {
		return values;
	}

	public void setValues(ArrayList<String> values) {
		this.values = values;
	}

	public ArrayList<String> getTypes() {
		return types;
	}

	public void setTypes(ArrayList<String> types) {
		this.types = types;
	}

	public ArrayList<Integer> getIndexes() {
		return indexes;
	}

	public void setIndexes(ArrayList<Integer> indexes) {
		this.indexes = indexes;
	}
}
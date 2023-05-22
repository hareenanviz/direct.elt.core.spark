package com.anvizent.elt.core.spark.sink.config.bean;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class NoSQLConstantsConfigBean implements Serializable {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> fields;
	private ArrayList<Class<?>> types;
	private ArrayList<String> values;
	private ArrayList<String> literalFields;
	private ArrayList<Class<?>> literalTypes;
	private ArrayList<String> literalValues;
	private ArrayList<String> dateFormats;

	public ArrayList<String> getFields() {
		return fields;
	}

	public void setFields(ArrayList<String> fields) {
		this.fields = fields;
	}

	public ArrayList<String> getValues() {
		return values;
	}

	public void setValues(ArrayList<String> values) {
		this.values = values;
	}

	public ArrayList<Class<?>> getTypes() {
		return types;
	}

	public void setTypes(ArrayList<Class<?>> types) {
		this.types = types;
	}

	public ArrayList<String> getLiteralFields() {
		return literalFields;
	}

	public void setLiteralFields(ArrayList<String> literalFields) {
		this.literalFields = literalFields;
	}

	public ArrayList<String> getLiteralValues() {
		return literalValues;
	}

	public void setLiteralValues(ArrayList<String> literalValues) {
		this.literalValues = literalValues;
	}

	public ArrayList<Class<?>> getLiteralTypes() {
		return literalTypes;
	}

	public void setLiteralTypes(ArrayList<Class<?>> literalTypes) {
		this.literalTypes = literalTypes;
	}

	public ArrayList<String> getDateFormats() {
		return dateFormats;
	}

	public void setDateFormats(ArrayList<String> dateFormats) {
		this.dateFormats = dateFormats;
	}

	public NoSQLConstantsConfigBean() {
	}

	public NoSQLConstantsConfigBean(ArrayList<String> fields, ArrayList<Class<?>> types, ArrayList<String> values, ArrayList<String> literalFields,
	        ArrayList<Class<?>> literalTypes, ArrayList<String> literalValues, ArrayList<String> dateFormats) {
		this.fields = fields;
		this.types = types;
		this.values = values;
		this.literalFields = literalFields;
		this.literalTypes = literalTypes;
		this.literalValues = literalValues;
		this.dateFormats = dateFormats;
	}
}

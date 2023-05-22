package com.anvizent.elt.core.spark.operation.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SequenceConfigBean extends ConfigBean implements SimpleOperationConfigBean {
	private static final long serialVersionUID = 1L;

	private ArrayList<String> fields;
	private ArrayList<Integer> fieldIndexes;
	private ArrayList<Integer> initialValues;

	public ArrayList<String> getFields() {
		return fields;
	}

	public void setFields(ArrayList<String> fields) {
		this.fields = fields;
	}

	public ArrayList<Integer> getFieldIndexes() {
		return fieldIndexes;
	}

	public void setFieldIndexes(ArrayList<Integer> fieldIndexes) {
		this.fieldIndexes = fieldIndexes;
	}

	public ArrayList<Integer> getInitialValues() {
		return initialValues;
	}

	public void setInitialValues(ArrayList<Integer> initialValues) {
		this.initialValues = initialValues;
	}
}
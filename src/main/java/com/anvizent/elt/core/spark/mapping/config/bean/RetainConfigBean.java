package com.anvizent.elt.core.spark.mapping.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RetainConfigBean extends MappingConfigBean {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> retainFields;
	private ArrayList<String> retainFieldsAs;
	private ArrayList<Integer> retainFieldsAt;
	private ArrayList<String> emitFields;

	public ArrayList<String> getRetainFields() {
		return retainFields;
	}

	public void setRetainFields(ArrayList<String> retainFields) {
		this.retainFields = retainFields;
	}

	public ArrayList<String> getRetainFieldsAs() {
		return retainFieldsAs;
	}

	public void setRetainFieldsAs(ArrayList<String> retainFieldsAs) {
		this.retainFieldsAs = retainFieldsAs;
	}

	public ArrayList<Integer> getRetainFieldsAt() {
		return retainFieldsAt;
	}

	public void setRetainFieldsAt(ArrayList<Integer> retainFieldsAt) {
		this.retainFieldsAt = retainFieldsAt;
	}

	public ArrayList<String> getEmitFields() {
		return emitFields;
	}

	public void setEmitFields(ArrayList<String> emitFields) {
		this.emitFields = emitFields;
	}

	public RetainConfigBean(ArrayList<String> retainFields, ArrayList<String> retainFieldsAs, ArrayList<Integer> retainFieldsAt, ArrayList<String> emitFields) {
		this.retainFields = retainFields;
		this.retainFieldsAs = retainFieldsAs;
		this.retainFieldsAt = retainFieldsAt;
		this.emitFields = emitFields;
	}
}

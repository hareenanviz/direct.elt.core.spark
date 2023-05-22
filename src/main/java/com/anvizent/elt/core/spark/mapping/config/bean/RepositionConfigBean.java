package com.anvizent.elt.core.spark.mapping.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RepositionConfigBean extends MappingConfigBean {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> fields;
	private ArrayList<Integer> positions;

	public ArrayList<String> getFields() {
		return fields;
	}

	public void setFields(ArrayList<String> fields) {
		this.fields = fields;
	}

	public ArrayList<Integer> getPositions() {
		return positions;
	}

	public void setPositions(ArrayList<Integer> positions) {
		this.positions = positions;
	}

	public RepositionConfigBean(ArrayList<String> fields, ArrayList<Integer> positions) {
		this.fields = fields;
		this.positions = positions;
	}
}

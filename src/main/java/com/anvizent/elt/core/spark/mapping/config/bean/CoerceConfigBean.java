package com.anvizent.elt.core.spark.mapping.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class CoerceConfigBean extends MappingConfigBean {
	private static final long serialVersionUID = 1L;

	private ArrayList<String> fields;
	private ArrayList<Class<?>> types;
	private ArrayList<String> formats;
	private ArrayList<Integer> precisions;
	private ArrayList<Integer> scales;

	public ArrayList<String> getFields() {
		return fields;
	}

	public void setFields(ArrayList<String> fields) {
		this.fields = fields;
	}

	public ArrayList<Class<?>> getTypes() {
		return types;
	}

	public void setTypes(ArrayList<Class<?>> types) {
		this.types = types;
	}

	public ArrayList<String> getFormats() {
		return formats;
	}

	public void setFormats(ArrayList<String> formats) {
		this.formats = formats;
	}

	public ArrayList<Integer> getPrecisions() {
		return precisions;
	}

	public void setPrecisions(ArrayList<Integer> precisions) {
		this.precisions = precisions;
	}

	public ArrayList<Integer> getScales() {
		return scales;
	}

	public void setScales(ArrayList<Integer> scales) {
		this.scales = scales;
	}

	public CoerceConfigBean(ArrayList<String> fields, ArrayList<Class<?>> types, ArrayList<String> formats, ArrayList<Integer> precisions,
	        ArrayList<Integer> scales) {
		this.fields = fields;
		this.types = types;
		this.formats = formats;
		this.precisions = precisions;
		this.scales = scales;
	}
}

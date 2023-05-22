package com.anvizent.elt.core.spark.filter.config.bean;

import java.util.ArrayList;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FilterByRegExConfigBean extends FilterByExpressionConfigBean implements IFilterConfigBean {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> emitStreamNames;
	private ArrayList<String> regExps;
	private boolean ignoreRowIfNull;

	@Override
	public ArrayList<String> getEmitStreamNames() {
		return emitStreamNames;
	}

	@Override
	public void setEmitStreamNames(ArrayList<String> emitStreamNames) {
		this.emitStreamNames = emitStreamNames;
	}

	public ArrayList<String> getRegExps() {
		return regExps;
	}

	public void setRegExps(ArrayList<String> regExps) {
		this.regExps = regExps;
	}

	public boolean isIgnoreRowIfNull() {
		return ignoreRowIfNull;
	}

	public void setIgnoreRowIfNull(boolean ignoreRowIfNull) {
		this.ignoreRowIfNull = ignoreRowIfNull;
	}
}

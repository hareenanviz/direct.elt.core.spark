package com.anvizent.elt.core.spark.filter.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.spark.operation.config.bean.JavaExpressionBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FilterByExpressionConfigBean extends JavaExpressionBean implements IFilterConfigBean {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> emitStreamNames;

	@Override
	public ArrayList<String> getEmitStreamNames() {
		return emitStreamNames;
	}

	@Override
	public void setEmitStreamNames(ArrayList<String> emitStreamNames) {
		this.emitStreamNames = emitStreamNames;
	}
}
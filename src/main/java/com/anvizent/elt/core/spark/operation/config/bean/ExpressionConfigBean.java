package com.anvizent.elt.core.spark.operation.config.bean;

import java.util.ArrayList;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ExpressionConfigBean extends JavaExpressionBean {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> expressionsFieldNames;
	private ArrayList<Integer> expressionsFieldIndexes;
	private ArrayList<Class<?>> returnTypes;
	private ArrayList<Integer> precisions;
	private ArrayList<Integer> scales;

	public ArrayList<String> getExpressionsFieldNames() {
		return expressionsFieldNames;
	}

	public void setExpressionsFieldNames(ArrayList<String> expressionsFieldNames) {
		this.expressionsFieldNames = expressionsFieldNames;
	}

	public ArrayList<Class<?>> getReturnTypes() {
		return returnTypes;
	}

	public void setReturnTypes(ArrayList<Class<?>> returnTypes) {
		this.returnTypes = returnTypes;
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

	public ArrayList<Integer> getExpressionsFieldIndexes() {
		return expressionsFieldIndexes;
	}

	public void setExpressionsFieldIndexes(ArrayList<Integer> expressionsFieldIndexes) {
		this.expressionsFieldIndexes = expressionsFieldIndexes;
	}

}

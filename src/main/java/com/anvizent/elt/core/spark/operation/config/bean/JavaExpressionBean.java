package com.anvizent.elt.core.spark.operation.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class JavaExpressionBean extends ConfigBean implements SimpleOperationConfigBean {
	private static final long serialVersionUID = 1L;

	private ArrayList<String> expressions;
	private ArrayList<String> argumentFields;
	private ArrayList<Class<?>> argumentTypes;

	private ArrayList<ArrayList<String>> argumentFieldAliases = new ArrayList<>();
	private ArrayList<ArrayList<Class<?>>> argumentTypesByExpression = new ArrayList<>();
	private ArrayList<ArrayList<String>> argumentFieldsByExpression = new ArrayList<>();

	public ArrayList<String> getExpressions() {
		return expressions;
	}

	public void setExpressions(ArrayList<String> expressions) {
		this.expressions = expressions;
	}

	public ArrayList<String> getArgumentFields() {
		return argumentFields;
	}

	public void setArgumentFields(ArrayList<String> argumentFields) {
		this.argumentFields = argumentFields;
	}

	public ArrayList<Class<?>> getArgumentTypes() {
		return argumentTypes;
	}

	public void setArgumentTypes(ArrayList<Class<?>> argumentTypes) {
		this.argumentTypes = argumentTypes;
	}

	public ArrayList<ArrayList<String>> getArgumentFieldAliases() {
		return argumentFieldAliases;
	}

	public void setArgumentFieldAliases(ArrayList<ArrayList<String>> argumentFieldAliases) {
		this.argumentFieldAliases = argumentFieldAliases;
	}

	public ArrayList<ArrayList<Class<?>>> getArgumentTypesByExpression() {
		return argumentTypesByExpression;
	}

	public void setArgumentTypesByExpression(ArrayList<ArrayList<Class<?>>> argumentTypesByExpression) {
		this.argumentTypesByExpression = argumentTypesByExpression;
	}

	public ArrayList<ArrayList<String>> getArgumentFieldsByExpression() {
		return argumentFieldsByExpression;
	}

	public void setArgumentFieldsByExpression(ArrayList<ArrayList<String>> argumentFieldsByExpression) {
		this.argumentFieldsByExpression = argumentFieldsByExpression;
	}
}
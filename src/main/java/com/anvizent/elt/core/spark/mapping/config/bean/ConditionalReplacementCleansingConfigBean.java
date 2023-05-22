package com.anvizent.elt.core.spark.mapping.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.spark.constant.CleansingValidationType;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ConditionalReplacementCleansingConfigBean extends MappingConfigBean {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> fields;
	private ArrayList<CleansingValidationType> validationTypes;
	private ArrayList<String> replacementValues;
	private ArrayList<String> replacementValuesByFields;
	private ArrayList<String> dateFormats;
	private ArrayList<String> min;
	private ArrayList<String> max;
	private ArrayList<String> equals;
	private ArrayList<String> notEquals;
	private ArrayList<String> matchesRegex;
	private ArrayList<String> notMatchesRegex;

	private ArrayList<String> expressions;
	private ArrayList<String> argumentFields;
	private ArrayList<Class<?>> argumentTypes;

	private ArrayList<ArrayList<String>> argumentFieldAliases = new ArrayList<>();
	private ArrayList<ArrayList<Class<?>>> argumentTypesByExpression = new ArrayList<>();
	private ArrayList<ArrayList<String>> argumentFieldsByExpression = new ArrayList<>();

	public ArrayList<String> getFields() {
		return fields;
	}

	public void setFields(ArrayList<String> fields) {
		this.fields = fields;
	}

	public ArrayList<CleansingValidationType> getValidationTypes() {
		return validationTypes;
	}

	public void setValidationTypes(ArrayList<CleansingValidationType> validationTypes) {
		this.validationTypes = validationTypes;
	}

	public ArrayList<String> getReplacementValues() {
		return replacementValues;
	}

	public void setReplacementValues(ArrayList<String> replacementValues) {
		this.replacementValues = replacementValues;
	}

	public ArrayList<String> getReplacementValuesByFields() {
		return replacementValuesByFields;
	}

	public void setReplacementValuesByFields(ArrayList<String> replacementValuesByFields) {
		this.replacementValuesByFields = replacementValuesByFields;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public ArrayList<String> getDateFormats() {
		return dateFormats;
	}

	public void setDateFormats(ArrayList<String> dateFormats) {
		this.dateFormats = dateFormats;
	}

	public ArrayList<String> getMin() {
		return min;
	}

	public void setMin(ArrayList<String> min) {
		this.min = min;
	}

	public ArrayList<String> getMax() {
		return max;
	}

	public void setMax(ArrayList<String> max) {
		this.max = max;
	}

	public ArrayList<String> getEquals() {
		return equals;
	}

	public void setEquals(ArrayList<String> equals) {
		this.equals = equals;
	}

	public ArrayList<String> getNotEquals() {
		return notEquals;
	}

	public void setNotEquals(ArrayList<String> notEquals) {
		this.notEquals = notEquals;
	}

	public ArrayList<String> getMatchesRegex() {
		return matchesRegex;
	}

	public void setMatchesRegex(ArrayList<String> matchesRegex) {
		this.matchesRegex = matchesRegex;
	}

	public ArrayList<String> getNotMatchesRegex() {
		return notMatchesRegex;
	}

	public void setNotMatchesRegex(ArrayList<String> notMatchesRegex) {
		this.notMatchesRegex = notMatchesRegex;
	}

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
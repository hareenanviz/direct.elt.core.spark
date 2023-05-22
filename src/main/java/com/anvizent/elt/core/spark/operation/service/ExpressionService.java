package com.anvizent.elt.core.spark.operation.service;

import java.util.ArrayList;
import java.util.HashMap;

import org.codehaus.janino.ExpressionEvaluator;

import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.ExpressionConstant;
import com.anvizent.elt.core.spark.filter.config.bean.FilterByExpressionConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.JavaExpressionBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ExpressionService {

	public static String decodeExpression(String expression, ArrayList<String> argumentFieldsByExpression, ArrayList<Class<?>> argumentTypesByExpression,
	        ArrayList<Class<?>> argumentTypes, ArrayList<String> argumentFieldAliases, ArrayList<String> argumentFields) {
		if (argumentFields.size() > 0) {
			int argIndex = argumentFields.size() - 1;

			while (argIndex >= 0) {
				expression = decodeExpression(argIndex, expression, argumentFieldsByExpression, argumentTypesByExpression, argumentTypes, argumentFieldAliases,
				        argumentFields);
				argIndex--;
			}
		}

		return expression;
	}

	private static String decodeExpression(int argIndex, String expression, ArrayList<String> argumentFieldsByExpression,
	        ArrayList<Class<?>> argumentTypesByExpression, ArrayList<Class<?>> argumentTypes, ArrayList<String> argumentFieldAliases,
	        ArrayList<String> argumentFields) {
		String argument = ExpressionConstant.ARG + argIndex;

		if (expression.contains(ExpressionConstant.REPLACE_$ + argIndex)) {
			expression = expression.replace(ExpressionConstant.REPLACE_$ + argIndex, argument);
			argumentFieldsByExpression.add(0, argument);
			argumentTypesByExpression.add(0, argumentTypes.get(argIndex));
			argumentFieldAliases.add(0, argumentFields.get(argIndex));
		}

		return expression;
	}

	public static Object evaluateExpression(ExpressionEvaluator expEvaluatior, ArrayList<String> argumentFieldsByExpression,
	        ArrayList<String> argumentFieldAliases, HashMap<String, Object> row) throws DataCorruptedException, ValidationViolationException {
		Object expEvaluatorObj = null;

		try {
			Object[] expressionValues = getArgumentFieldValues(row, argumentFieldsByExpression, argumentFieldAliases);
			expEvaluatorObj = expEvaluatior.evaluate(expressionValues);
		} catch (Exception exception) {
			throw new DataCorruptedException(exception);
		}

		return expEvaluatorObj;
	}

	private static Object[] getArgumentFieldValues(HashMap<String, Object> expressionValues, ArrayList<String> argumentFieldsByExpression,
	        ArrayList<String> argumentFieldAliases) {
		Object[] argumentFieldValueObject = new Object[argumentFieldsByExpression.size()];

		for (int i = 0; i < argumentFieldsByExpression.size(); i++) {
			argumentFieldValueObject[i] = expressionValues.get(argumentFieldAliases.get(i));
		}

		return argumentFieldValueObject;
	}

	public static void decodeIfElseExpressions(FilterByExpressionConfigBean configBean) {
		String previousExpression = "", currentExpression = null;
		boolean addAnd = false;

		for (int i = 0; i < configBean.getExpressions().size(); i++) {
			addAnd = i > 0;

			currentExpression = configBean.getExpressions().get(i);
			configBean.getExpressions().set(i, previousExpression + (addAnd ? " && " : "") + currentExpression);

			addArgumentsNewList(configBean);

			configBean.getExpressions().set(i,
			        ExpressionService.decodeExpression(configBean.getExpressions().get(i), configBean.getArgumentFieldsByExpression().get(i),
			                configBean.getArgumentTypesByExpression().get(i), configBean.getArgumentTypes(), configBean.getArgumentFieldAliases().get(i),
			                configBean.getArgumentFields()));

			previousExpression += (addAnd ? " && " : "") + "!(" + currentExpression + ")";
		}

		if (configBean.getEmitStreamNames().size() > configBean.getExpressions().size()) {
			int index = configBean.getExpressions().size();
			addExpression(configBean, index, previousExpression);
		}
	}

	private static void addExpression(FilterByExpressionConfigBean configBean, int index, String expression) {
		addArgumentsNewList(configBean);

		configBean.getExpressions()
		        .add(ExpressionService.decodeExpression(expression, configBean.getArgumentFieldsByExpression().get(index),
		                configBean.getArgumentTypesByExpression().get(index), configBean.getArgumentTypes(), configBean.getArgumentFieldAliases().get(index),
		                configBean.getArgumentFields()));
	}

	private static void addArgumentsNewList(FilterByExpressionConfigBean configBean) {
		configBean.getArgumentFieldAliases().add(new ArrayList<>());
		configBean.getArgumentTypesByExpression().add(new ArrayList<>());
		configBean.getArgumentFieldsByExpression().add(new ArrayList<>());
	}

	public static void decodeExpressions(JavaExpressionBean configBean) {
		for (int i = 0; i < configBean.getExpressions().size(); i++) {
			configBean.getArgumentFieldAliases().add(new ArrayList<>());
			configBean.getArgumentTypesByExpression().add(new ArrayList<>());
			configBean.getArgumentFieldsByExpression().add(new ArrayList<>());

			configBean.getExpressions().set(i,
			        ExpressionService.decodeExpression(configBean.getExpressions().get(i), configBean.getArgumentFieldsByExpression().get(i),
			                configBean.getArgumentTypesByExpression().get(i), configBean.getArgumentTypes(), configBean.getArgumentFieldAliases().get(i),
			                configBean.getArgumentFields()));
		}
	}
}

package com.anvizent.elt.core.spark.filter.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentFilterFunction;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.spark.filter.config.bean.FilterByExpressionConfigBean;
import com.anvizent.elt.core.spark.operation.service.ExpressionService;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FilterByExpressionFunction extends AnvizentFilterFunction {

	private static final long serialVersionUID = 1L;

	private ExpressionEvaluator expressionEvaluator;
	private Integer expressionIndex;

	public FilterByExpressionFunction(FilterByExpressionConfigBean filterByExpressionBean, Integer expressionIndex,
	        LinkedHashMap<String, AnvizentDataType> structure, LinkedHashMap<String, AnvizentDataType> newStructure,
	        ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws InvalidArgumentsException {
		super(filterByExpressionBean, null, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
		this.expressionIndex = expressionIndex;
	}

	@Override
	public Boolean process(HashMap<String, Object> row) throws ValidationViolationException, DataCorruptedException, RecordProcessingException {
		FilterByExpressionConfigBean filterByExpressionBean = (FilterByExpressionConfigBean) configBean;
		initExpressionEvaluator(filterByExpressionBean);

		return (Boolean) ExpressionService.evaluateExpression(expressionEvaluator, filterByExpressionBean.getArgumentFieldsByExpression().get(expressionIndex),
		        filterByExpressionBean.getArgumentFieldAliases().get(expressionIndex), row);
	}

	private void initExpressionEvaluator(FilterByExpressionConfigBean filterByExpressionBean) throws ValidationViolationException {
		if (expressionEvaluator == null) {
			try {
				expressionEvaluator = getExpressionEvaluator(filterByExpressionBean.getExpressions().get(expressionIndex), Boolean.class,
				        filterByExpressionBean.getArgumentFieldsByExpression().get(expressionIndex),
				        filterByExpressionBean.getArgumentTypesByExpression().get(expressionIndex),
				        filterByExpressionBean.getArgumentFieldAliases().get(expressionIndex));
			} catch (CompileException e) {
				throw new ValidationViolationException("Invalid expression, details: ", e);
			}
		}
	}

	private ExpressionEvaluator getExpressionEvaluator(String expression, Class<?> returnType, ArrayList<String> argumentFieldsByExpression,
	        ArrayList<Class<?>> argumentTypesByExpression, ArrayList<String> argumentFieldAliases) throws CompileException {
		return new ExpressionEvaluator(expression, returnType, argumentFieldsByExpression.toArray(new String[argumentFieldsByExpression.size()]),
		        argumentTypesByExpression.toArray(new Class[argumentTypesByExpression.size()]), new Class[] { Exception.class },
		        Exception.class.getClassLoader());
	}
}
package com.anvizent.elt.core.spark.operation.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentFunction;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.spark.operation.config.bean.ExpressionConfigBean;
import com.anvizent.elt.core.spark.operation.service.ExpressionService;
import com.anvizent.elt.core.spark.util.RowUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ExpressionFunction extends AnvizentFunction {

	private static final long serialVersionUID = 1L;
	private List<ExpressionEvaluator> expressionEvaluators;

	public ExpressionFunction(ExpressionConfigBean expressionBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws InvalidArgumentsException {
		super(expressionBean, null, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public HashMap<String, Object> process(HashMap<String, Object> row) throws ValidationViolationException, DataCorruptedException {
		ExpressionConfigBean expressionConfigBean = (ExpressionConfigBean) configBean;
		LinkedHashMap<String, Object> newRow = new LinkedHashMap<>();

		initExpressionEvaluators(expressionConfigBean);

		for (int i = 0; i < expressionConfigBean.getExpressions().size(); i++) {
			newRow.put(expressionConfigBean.getExpressionsFieldNames().get(i), ExpressionService.evaluateExpression(expressionEvaluators.get(i),
			        expressionConfigBean.getArgumentFieldsByExpression().get(i), expressionConfigBean.getArgumentFieldAliases().get(i), row));
		}

		return RowUtil.addElements(row, newRow, newStructure);
	}

	private void initExpressionEvaluators(ExpressionConfigBean expressionConfigBean) throws ValidationViolationException {
		if (expressionEvaluators == null || expressionEvaluators.isEmpty()) {
			if (expressionEvaluators == null) {
				expressionEvaluators = new ArrayList<>();
			}

			for (int i = 0; i < expressionConfigBean.getExpressions().size(); i++) {
				try {
					expressionEvaluators.add(getExpressionEvaluator(expressionConfigBean.getExpressions().get(i), expressionConfigBean.getReturnTypes().get(i),
					        expressionConfigBean.getArgumentFieldsByExpression().get(i), expressionConfigBean.getArgumentTypesByExpression().get(i),
					        expressionConfigBean.getArgumentFieldAliases().get(i)));
				} catch (CompileException e) {
					throw new ValidationViolationException("Invalid expression, details: ", e);
				}
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

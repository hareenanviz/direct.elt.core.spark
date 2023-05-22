package com.anvizent.elt.core.spark.sink.function;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.stats.beans.WrittenRow;
import com.anvizent.elt.core.spark.constant.EmptyConstants;
import com.anvizent.elt.core.spark.sink.config.bean.NoSQLConstantsConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.RethinkDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.service.RethinkDBSinkService;
import com.anvizent.elt.core.spark.sink.util.bean.RethinkDBSinkGetResult;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.model.MapObject;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public abstract class RethinkDBSinkFunction extends AnvizentVoidFunction {

	private static final long serialVersionUID = 1L;
	protected ArrayList<ExpressionEvaluator> expressionEvaluators;
	protected ArrayList<ExpressionEvaluator> insertExpressionEvaluators;
	protected ArrayList<ExpressionEvaluator> updateExpressionEvaluators;

	protected RethinkDBSinkConfigBean rethinkDBSinkConfigBean;
	private Table table;

	public RethinkDBSinkFunction(RethinkDBSinkConfigBean rethinkDBConfigBean, MappingConfigBean mappingConfigBean,
	        LinkedHashMap<String, AnvizentDataType> structure, LinkedHashMap<String, AnvizentDataType> newStructure,
	        ArrayList<AnvizentAccumulator> anvizentAccumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws InvalidRelationException {
		super(rethinkDBConfigBean, null, structure, newStructure, null, anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
		this.rethinkDBSinkConfigBean = rethinkDBConfigBean;
	}

	@Override
	public WrittenRow process(HashMap<String, Object> row) throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		try {
			Object rId = RethinkDBSinkService.getOrGenerateId(rethinkDBSinkConfigBean, row);
			boolean canAvoidSelect = RethinkDBSinkService.canAvoidSelect(rethinkDBSinkConfigBean, rId);
			RethinkDBSinkGetResult rethinkDBSinkGetResult = RethinkDBSinkService.getRethinkDBSinkGetResult(rethinkDBSinkConfigBean, row, getTable(), rId,
			        canAvoidSelect);

			initExpressionEvaluators(rethinkDBSinkConfigBean.getConstantsConfigBean(), expressionEvaluators);
			initExpressionEvaluators(rethinkDBSinkConfigBean.getInsertConstantsConfigBean(), insertExpressionEvaluators);
			initExpressionEvaluators(rethinkDBSinkConfigBean.getUpdateConstantsConfigBean(), updateExpressionEvaluators);
			return process(row, rethinkDBSinkGetResult);
		} catch (TimeoutException | SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	private void initExpressionEvaluators(NoSQLConstantsConfigBean constantsConfigBean, ArrayList<ExpressionEvaluator> expressionEvaluators)
	        throws ValidationViolationException {
		if (constantsConfigBean == null) {
			return;
		}

		if (expressionEvaluators == null || expressionEvaluators.isEmpty()) {
			if (expressionEvaluators == null) {
				expressionEvaluators = new ArrayList<>();
			}

			if (constantsConfigBean != null && constantsConfigBean.getValues() != null) {
				for (int i = 0; i < constantsConfigBean.getValues().size(); i++) {
					try {
						expressionEvaluators.add(getExpressionEvaluator(constantsConfigBean.getValues().get(i), constantsConfigBean.getTypes().get(i),
						        EmptyConstants.STRING_LIST, EmptyConstants.TYPE_LIST, EmptyConstants.STRING_LIST));
					} catch (CompileException e) {
						throw new ValidationViolationException("Invalid expression, details: ", e);
					}
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

	protected Table getTable() {
		if (table == null) {
			table = RethinkDB.r.table(rethinkDBSinkConfigBean.getTableName());
		}

		return table;
	}

	@SuppressWarnings("unchecked")
	protected static MapObject getRRecord(HashMap<String, Object> row) {
		MapObject rRow = RethinkDB.r.hashMap();
		rRow.putAll(row);
		return rRow;
	}

	public abstract WrittenRow process(HashMap<String, Object> row, RethinkDBSinkGetResult rethinkDBSinkGetResult)
	        throws RecordProcessingException, ValidationViolationException, DataCorruptedException;
}

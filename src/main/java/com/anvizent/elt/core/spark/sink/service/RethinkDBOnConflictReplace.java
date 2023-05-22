package com.anvizent.elt.core.spark.sink.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;

import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.spark.constant.EmptyConstants;
import com.anvizent.elt.core.spark.sink.config.bean.NoSQLConstantsConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.RethinkDBSinkConfigBean;
import com.anvizent.elt.core.spark.util.CollectionUtil;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction3;
import com.rethinkdb.model.MapObject;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RethinkDBOnConflictReplace implements ReqlFunction3 {

	private RethinkDBSinkConfigBean rethinkDBSinkConfigBean;
	private ArrayList<ExpressionEvaluator> expressionEvaluators;

	public RethinkDBOnConflictReplace(RethinkDBSinkConfigBean rethinkDBSinkConfigBean) {
		this.rethinkDBSinkConfigBean = rethinkDBSinkConfigBean;
	}

	@Override
	public Object apply(ReqlExpr id, ReqlExpr oldDoc, ReqlExpr newDoc) {
		try {
			initExpressionEvaluators(rethinkDBSinkConfigBean.getUpdateConstantsConfigBean());
		} catch (ValidationViolationException e) {
			throw new RuntimeException(e);
		}

		try {
			MapObject mapObject = RethinkDB.r.hashMap();

			setReplacementValues(mapObject);

			ReqlExpr resolvedDoc = newDoc.merge(mapObject);

			return resolvedDoc;
		} catch (UnsupportedCoerceException | InvalidSituationException | DateParseException | ImproperValidationException | DataCorruptedException
		        | ValidationViolationException | InvalidConfigValueException exception) {
			throw new RuntimeException(exception.getMessage(), exception);
		}
	}

	private void initExpressionEvaluators(NoSQLConstantsConfigBean constantsConfigBean) throws ValidationViolationException {
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

	private void setReplacementValues(MapObject mapObject) throws UnsupportedCoerceException, InvalidSituationException, DateParseException,
	        ImproperValidationException, DataCorruptedException, ValidationViolationException, InvalidConfigValueException {
		if (rethinkDBSinkConfigBean.getUpdateConstantsConfigBean() != null) {
			LinkedHashMap<String, Object> updateConstants = NoSQLConstantsService.getConstants(rethinkDBSinkConfigBean.getExternalDataPrefix(),
			        rethinkDBSinkConfigBean.getEmptyRow(), rethinkDBSinkConfigBean.getEmptyArguments(), rethinkDBSinkConfigBean.getTimeZoneOffset(),
			        rethinkDBSinkConfigBean.getUpdateConstantsConfigBean(), expressionEvaluators);
			RethinkDBSinkService.convertToRethinkDBType(updateConstants, rethinkDBSinkConfigBean.getTimeZoneOffset());

			replaceUpdateConstants(mapObject, updateConstants);
		}
	}

	@SuppressWarnings("unchecked")
	private void replaceUpdateConstants(MapObject mapObject, LinkedHashMap<String, Object> updateConstants) {
		mapObject.putAll(updateConstants);
		if (rethinkDBSinkConfigBean.getInsertConstantsConfigBean() != null) {
			CollectionUtil.putConstants(mapObject, rethinkDBSinkConfigBean.getInsertConstantsConfigBean().getFields(), null);
			CollectionUtil.putConstants(mapObject, rethinkDBSinkConfigBean.getInsertConstantsConfigBean().getLiteralFields(), null);
		}
	}
}

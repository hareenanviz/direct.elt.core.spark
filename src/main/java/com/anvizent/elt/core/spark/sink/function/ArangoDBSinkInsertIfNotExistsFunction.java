package com.anvizent.elt.core.spark.sink.function;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.spark.TaskContext;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.stats.beans.DBWrittenRow;
import com.anvizent.elt.core.lib.stats.beans.WrittenRow;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.ArangoDBConnectionByTaskId;
import com.anvizent.elt.core.spark.constant.EmptyConstants;
import com.anvizent.elt.core.spark.sink.config.bean.ArangoDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.NoSQLConstantsConfigBean;
import com.anvizent.elt.core.spark.sink.service.ArangoDBSinkService;
import com.anvizent.elt.core.spark.sink.service.NoSQLConstantsService;
import com.anvizent.elt.core.spark.sink.util.bean.ArangoDBSinkGetResult;
import com.anvizent.elt.core.spark.util.RowUtil;
import com.anvizent.query.builder.exception.UnderConstructionException;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.entity.BaseDocument;

/**
 * @author Hareen Bejjanki
 *
 */
public class ArangoDBSinkInsertIfNotExistsFunction extends ArangoDBSinkFunction {

	private static final long serialVersionUID = 1L;
	private ArrayList<ExpressionEvaluator> expressionEvaluators;

	public ArangoDBSinkInsertIfNotExistsFunction(ConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws InvalidRelationException {
		super(configBean, structure, newStructure, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	protected WrittenRow process(HashMap<String, Object> row, ArangoDBSinkGetResult arangoDBSinkGetResult)
	        throws RecordProcessingException, DataCorruptedException, ValidationViolationException {
		initExpressionEvaluators(((ArangoDBSinkConfigBean) configBean).getConstantsConfigBean());

		try {
			HashMap<String, Object> insertedRow = null;

			if (arangoDBSinkGetResult.getResult() == null) {
				insertedRow = insert(row, arangoDBSinkGetResult.getKey());
			}

			return new DBWrittenRow(newStructure, row, insertedRow, false);
		} catch (DataCorruptedException | UnsupportedCoerceException | InvalidSituationException exception) {
			throw new DataCorruptedException(exception);
		} catch (ValidationViolationException | DateParseException | InvalidConfigValueException | ImproperValidationException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		} catch (Exception exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
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

	private HashMap<String, Object> insert(HashMap<String, Object> row, String arangoDBKey)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, DataCorruptedException,
	        ValidationViolationException, InvalidConfigValueException, UnderConstructionException, IOException, SQLException, TimeoutException {
		HashMap<String, Object> returnRow;

		ArangoDBSinkConfigBean arangoDBSinkConfigBean = (ArangoDBSinkConfigBean) configBean;

		HashMap<String, Object> differKeysAndFieldsRow = RowUtil.changeFieldsToDifferColumns(row, arangoDBSinkConfigBean.getKeyFields(),
		        arangoDBSinkConfigBean.getKeyColumns(), arangoDBSinkConfigBean.getFieldsDifferToColumns(), arangoDBSinkConfigBean.getColumnsDifferToFields());

		HashMap<String, Object> newRow = RowUtil
		        .addElements(
		                differKeysAndFieldsRow, NoSQLConstantsService.getConstants(null, arangoDBSinkConfigBean.getEmptyRow(),
		                        arangoDBSinkConfigBean.getEmptyArguments(), null, arangoDBSinkConfigBean.getConstantsConfigBean(), expressionEvaluators),
		                newStructure);

		ArangoDBSinkService.convertToArangoDBType(newRow);
		ArangoDBSinkService.ifKeyExistsThenPut(newRow, arangoDBKey);

		String arangoDBInsertIfNotExistsQuery = ArangoDBSinkService.getInsertIfNotExistsQuery(arangoDBKey,
		        arangoDBSinkConfigBean.getKeyColumns() == null || arangoDBSinkConfigBean.getKeyColumns().isEmpty() ? arangoDBSinkConfigBean.getKeyFields()
		                : arangoDBSinkConfigBean.getKeyColumns(),
		        newRow, arangoDBSinkConfigBean.getTableName(), arangoDBSinkConfigBean.isWaitForSync());

		ArangoDB arangoDBConnection = ((ArangoDB) ApplicationConnectionBean.getInstance()
		        .get(new ArangoDBConnectionByTaskId(arangoDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0]);
		ArangoCursor<BaseDocument> arangoDBCursor = arangoDBConnection.db(arangoDBSinkConfigBean.getConnection().getDBName())
		        .query(arangoDBInsertIfNotExistsQuery, null, null, BaseDocument.class);
		if (arangoDBCursor.hasNext()) {
			if (arangoDBCursor.next().getProperties().get("old") == null) {
				returnRow = newRow;
			} else {
				returnRow = null;
			}
			arangoDBCursor.close();
			arangoDBConnection.shutdown();
		} else {
			returnRow = null;
		}

		return returnRow;
	}
}

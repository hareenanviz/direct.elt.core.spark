package com.anvizent.elt.core.spark.sink.function;

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
import com.anvizent.elt.core.spark.constant.Constants.NOSQL;
import com.anvizent.elt.core.spark.constant.EmptyConstants;
import com.anvizent.elt.core.spark.exception.RecordDoesNotExistsForUpdateException;
import com.anvizent.elt.core.spark.sink.config.bean.ArangoDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.NoSQLConstantsConfigBean;
import com.anvizent.elt.core.spark.sink.service.ArangoDBSinkService;
import com.anvizent.elt.core.spark.sink.service.NoSQLConstantsService;
import com.anvizent.elt.core.spark.sink.util.bean.ArangoDBSinkGetResult;
import com.anvizent.elt.core.spark.util.RowUtil;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.DocumentUpdateEntity;
import com.arangodb.model.DocumentUpdateOptions;

/**
 * @author Hareen Bejjanki
 *
 */
public class ArangoDBSinkUpdateFunction extends ArangoDBSinkFunction {

	private static final long serialVersionUID = 1L;
	private ArrayList<ExpressionEvaluator> expressionEvaluators;

	public ArangoDBSinkUpdateFunction(ConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws InvalidRelationException {
		super(configBean, structure, newStructure, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	protected WrittenRow process(HashMap<String, Object> row, ArangoDBSinkGetResult arangoDBSinkGetResult)
	        throws RecordProcessingException, DataCorruptedException, ValidationViolationException {
		initExpressionEvaluators(((ArangoDBSinkConfigBean) configBean).getConstantsConfigBean());

		try {
			if (((!row.containsKey(NOSQL.ARANGO_DB_KEY) && !((ArangoDBSinkConfigBean) configBean).isGenerateId()))
			        && arangoDBSinkGetResult.getResult() == null) {
				throw new RecordDoesNotExistsForUpdateException(
				        "Record does not exits in '" + ((ArangoDBSinkConfigBean) configBean).getTableName() + "' target table for update.");
			} else {
				return update(row, arangoDBSinkGetResult);
			}
		} catch (DataCorruptedException | UnsupportedCoerceException | InvalidSituationException | RecordDoesNotExistsForUpdateException exception) {
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

	private WrittenRow update(HashMap<String, Object> row, ArangoDBSinkGetResult arangoDBSinkGetResult)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, DataCorruptedException,
	        ValidationViolationException, InvalidConfigValueException, ArangoDBException, SQLException, TimeoutException {
		if ((arangoDBSinkGetResult.getKey() != null && !arangoDBSinkGetResult.getKey().isEmpty()) || arangoDBSinkGetResult.isDoUpdate()) {
			return update(row, arangoDBSinkGetResult.getKey());
		} else {
			return new DBWrittenRow(newStructure, row, null, false);
		}
	}

	private DBWrittenRow update(HashMap<String, Object> row, String arangoDBKey)
	        throws DataCorruptedException, UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException,
	        ValidationViolationException, InvalidConfigValueException, ArangoDBException, SQLException, TimeoutException {
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
		ArangoDBSinkService.skipMetaDataFields(newRow, arangoDBSinkConfigBean.getMetaDataFields());

		try {
			DocumentUpdateOptions documentUpdateOptions = new DocumentUpdateOptions().returnNew(true).returnOld(true);
			if (arangoDBSinkConfigBean.isWaitForSync()) {
				documentUpdateOptions = documentUpdateOptions.waitForSync(true);
			}

			DocumentUpdateEntity<HashMap<String, Object>> updateResult = ((ArangoDB) ApplicationConnectionBean.getInstance()
			        .get(new ArangoDBConnectionByTaskId(arangoDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0])
			                .db(arangoDBSinkConfigBean.getConnection().getDBName()).collection(arangoDBSinkConfigBean.getTableName())
			                .updateDocument(arangoDBKey, newRow, documentUpdateOptions);
			boolean updated = ArangoDBSinkService.verifyUpdate(updateResult.getOld(), updateResult.getNew(), arangoDBSinkConfigBean.getMetaDataFields(),
			        newStructure);
			if (updated) {
				return new DBWrittenRow(newStructure, row, newRow, true);
			} else {
				return new DBWrittenRow(newStructure, row, null, false);
			}
		} catch (ArangoDBException arangoDBException) {
			if (arangoDBException.getErrorNum() == 1202 && arangoDBException.getErrorMessage().contains("document not found")) {
				throw new DataCorruptedException("Record not found for update.", arangoDBException);
			} else {
				throw arangoDBException;
			}
		}

	}

}

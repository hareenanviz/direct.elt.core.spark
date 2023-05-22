package com.anvizent.elt.core.spark.sink.function;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import org.apache.spark.TaskContext;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.BatchTrackingCountConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.BaseAnvizentBatchFunction;
import com.anvizent.elt.core.lib.row.formatter.AnvizentErrorSetter;
import com.anvizent.elt.core.lib.row.formatter.BaseAnvizentErrorSetter;
import com.anvizent.elt.core.lib.stats.beans.DBWrittenStats;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.ArangoDBConnectionByTaskId;
import com.anvizent.elt.core.spark.constant.Constants.NOSQL;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.constant.EmptyConstants;
import com.anvizent.elt.core.spark.exception.RecordAlreadyExistsException;
import com.anvizent.elt.core.spark.exception.RecordDoesNotExistsForUpdateException;
import com.anvizent.elt.core.spark.sink.config.bean.ArangoDBBatchDataAndConnections;
import com.anvizent.elt.core.spark.sink.config.bean.ArangoDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.NoSQLConstantsConfigBean;
import com.anvizent.elt.core.spark.sink.service.ArangoDBSinkService;
import com.anvizent.elt.core.spark.sink.util.bean.ArangoDBSinkGetResult;
import com.anvizent.elt.core.spark.util.CollectionUtil;
import com.anvizent.query.builder.exception.InvalidInputException;
import com.anvizent.query.builder.exception.UnderConstructionException;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.entity.DocumentImportEntity;
import com.arangodb.entity.DocumentUpdateEntity;
import com.arangodb.entity.ErrorEntity;
import com.arangodb.entity.MultiDocumentEntity;
import com.arangodb.model.DocumentImportOptions;
import com.arangodb.model.DocumentUpdateOptions;

/**
 * @author Hareen Bejjanki
 *
 */
@SuppressWarnings({ "rawtypes" })
public class ArangoDBSinkBatchFunction extends BaseAnvizentBatchFunction<HashMap<String, Object>, HashMap<String, Object>> {
	private static final long serialVersionUID = 1L;

	private ArangoDBSinkConfigBean arangoDBSinkConfigBean;
	private ArangoDBBatchDataAndConnections arangoDBBatchDataAndConnections;
	private ArrayList<ExpressionEvaluator> expressionEvaluators;
	private ArrayList<ExpressionEvaluator> insertExpressionEvaluators;
	private ArrayList<ExpressionEvaluator> updateExpressionEvaluators;

	public ArangoDBSinkBatchFunction(ArangoDBSinkConfigBean arangoDBSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws InvalidRelationException, InvalidArgumentsException {
		super(arangoDBSinkConfigBean, null, structure, newStructure, null, arangoDBSinkConfigBean.getMaxRetryCount(), arangoDBSinkConfigBean.getRetryDelay(),
		        arangoDBSinkConfigBean.getInitRetryConfigBean().getMaxRetryCount(), arangoDBSinkConfigBean.getInitRetryConfigBean().getRetryDelay(),
		        arangoDBSinkConfigBean.getDestroyRetryConfigBean().getMaxRetryCount(), arangoDBSinkConfigBean.getDestroyRetryConfigBean().getRetryDelay(),
		        arangoDBSinkConfigBean.getBatchSize(), accumulators, errorHandlerSinkFunction, jobDetails);

		this.arangoDBSinkConfigBean = arangoDBSinkConfigBean;
		this.arangoDBBatchDataAndConnections = new ArangoDBBatchDataAndConnections();
	}

	@Override
	public void onCallStart(Iterator<HashMap<String, Object>> rows, BatchTrackingCountConfigBean trackingCounts) throws ImproperValidationException, Exception {
		if (arangoDBBatchDataAndConnections.getConnection(TaskContext.getPartitionId()) == null) {
			arangoDBBatchDataAndConnections.addConnection(TaskContext.getPartitionId(), getConnection());
		}
	}

	private ArangoDB getConnection() throws ImproperValidationException, UnimplementedException, Exception {
		try {
			return (ArangoDB) ApplicationConnectionBean.getInstance()
			        .get(new ArangoDBConnectionByTaskId(arangoDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0];
		} catch (TimeoutException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	@Override
	public boolean process(HashMap<String, Object> row, ArrayList<HashMap<String, Object>> batchedRows)
	        throws DataCorruptedException, RecordProcessingException, ValidationViolationException {
		initExpressionEvaluators(arangoDBSinkConfigBean.getConstantsConfigBean(), expressionEvaluators);
		initExpressionEvaluators(arangoDBSinkConfigBean.getInsertConstantsConfigBean(), insertExpressionEvaluators);
		initExpressionEvaluators(arangoDBSinkConfigBean.getUpdateConstantsConfigBean(), updateExpressionEvaluators);

		try {
			String arangoDBKey = ArangoDBSinkService.getOrGenerateId(arangoDBSinkConfigBean, row);
			boolean canAvoidSelect = ArangoDBSinkService.canAvoidSelect(arangoDBSinkConfigBean, arangoDBKey);

			if (keysProcessed(arangoDBKey, canAvoidSelect, row)) {
				return true;
			} else {
				return process(row, arangoDBKey, canAvoidSelect);
			}
		} catch (DataCorruptedException | UnsupportedCoerceException | InvalidSituationException | RecordAlreadyExistsException
		        | RecordDoesNotExistsForUpdateException exception) {
			throw new DataCorruptedException(exception);
		} catch (ValidationViolationException | DateParseException | InvalidConfigValueException | ImproperValidationException
		        | ClassNotFoundException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		} catch (Exception exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	private void initExpressionEvaluators(NoSQLConstantsConfigBean constantsConfigBean, ArrayList<ExpressionEvaluator> expressionEvaluators)
	        throws ValidationViolationException {
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

	private boolean keysProcessed(String arangoDBKey, boolean canAvoidSelect, HashMap<String, Object> row)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, DataCorruptedException,
	        InvalidConfigValueException, UnsupportedException, ValidationViolationException {
		if (arangoDBKey == null && !canAvoidSelect) {
			HashMap<String, Object> keys = getKeys(row);
			if (keys != null && arangoDBBatchDataAndConnections.getKeysAdded(TaskContext.getPartitionId()) != null
			        && arangoDBBatchDataAndConnections.getKeysAdded(TaskContext.getPartitionId()).contains(keys)) {
				HashMap<String, Object> record;

				if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
					record = ArangoDBSinkService.getRecord(arangoDBSinkConfigBean, row, arangoDBKey, arangoDBSinkConfigBean.getUpdateConstantsConfigBean(),
					        arangoDBSinkConfigBean.getMetaDataFields(), newStructure, updateExpressionEvaluators);
				} else {
					record = ArangoDBSinkService.getRecord(arangoDBSinkConfigBean, row, arangoDBKey, arangoDBSinkConfigBean.getConstantsConfigBean(),
					        arangoDBSinkConfigBean.getMetaDataFields(), newStructure, expressionEvaluators);
				}

				arangoDBBatchDataAndConnections.addUpdateData(TaskContext.getPartitionId(), record);
				return true;
			}
		}

		return false;
	}

	private HashMap<String, Object> getKeys(HashMap<String, Object> row) throws UnsupportedCoerceException, InvalidSituationException, DateParseException,
	        ImproperValidationException, InvalidConfigValueException, UnsupportedException, DataCorruptedException {
		if (arangoDBSinkConfigBean.getKeyFields() == null || arangoDBSinkConfigBean.getKeyFields().isEmpty()) {
			return null;
		} else {
			HashMap<String, Object> keys = new HashMap<>();

			for (int i = 0; i < arangoDBSinkConfigBean.getKeyFields().size(); i++) {
				keys.put(arangoDBSinkConfigBean.getKeyFields().get(i), row.get(arangoDBSinkConfigBean.getKeyFields().get(i)));
			}

			return keys;
		}
	}

	private boolean process(HashMap<String, Object> row, String arangoDBKey, boolean canAvoidSelect)
	        throws DataCorruptedException, ValidationViolationException, RecordDoesNotExistsForUpdateException, InvalidConfigValueException,
	        UnsupportedException, RecordProcessingException, TimeoutException, SQLException, RecordAlreadyExistsException, ClassNotFoundException,
	        InvalidInputException, UnderConstructionException, ParseException, IOException {
		ArangoDBSinkGetResult arangoDBSinkGetResult = ArangoDBSinkService.getRethinkDBSinkGetResult(arangoDBSinkConfigBean, row, arangoDBKey, canAvoidSelect);
		arangoDBBatchDataAndConnections.addSelectData(TaskContext.getPartitionId(), arangoDBSinkGetResult);

		if (arangoDBSinkGetResult.getResult() != null) {
			return processRecordSelectedAndExists(row, arangoDBKey, arangoDBSinkGetResult);
		} else if (arangoDBKey == null || arangoDBKey.isEmpty()) {
			return processRecordNotExistsWithoutArangoDBKey(row, arangoDBKey, arangoDBSinkGetResult);
		} else if (CollectionUtil.isEmpty(arangoDBSinkConfigBean.getMetaDataFields())) {
			return processRecordUnselectedAndNoMetaDataFields(row, arangoDBKey);
		} else {
			arangoDBBatchDataAndConnections.addUnSelectedArangoDBKeys(TaskContext.getPartitionId(), arangoDBKey, row);
			return true;
		}
	}

	private boolean processRecordUnselectedAndNoMetaDataFields(HashMap<String, Object> row, String arangoDBKey)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, DataCorruptedException,
	        ValidationViolationException, InvalidConfigValueException {
		if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)) {
			HashMap<String, Object> record = ArangoDBSinkService.getRecord(arangoDBSinkConfigBean, row, arangoDBKey,
			        arangoDBSinkConfigBean.getConstantsConfigBean(), arangoDBSinkConfigBean.getMetaDataFields(), newStructure, expressionEvaluators);

			arangoDBBatchDataAndConnections.addUpdateData(TaskContext.getPartitionId(), record);
		} else {
			HashMap<String, Object> record;

			if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
				record = ArangoDBSinkService.getRecord(arangoDBSinkConfigBean, row, arangoDBKey, arangoDBSinkConfigBean.getInsertConstantsConfigBean(),
				        arangoDBSinkConfigBean.getMetaDataFields(), newStructure, insertExpressionEvaluators);
			} else {
				record = ArangoDBSinkService.getRecord(arangoDBSinkConfigBean, row, arangoDBKey, arangoDBSinkConfigBean.getConstantsConfigBean(),
				        arangoDBSinkConfigBean.getMetaDataFields(), newStructure, expressionEvaluators);
			}

			arangoDBBatchDataAndConnections.addInsertData(TaskContext.getPartitionId(), record);
		}

		return true;
	}

	private boolean processRecordNotExistsWithoutArangoDBKey(HashMap<String, Object> row, String arangoDBKey, ArangoDBSinkGetResult arangoDBSinkGetResult)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, DataCorruptedException,
	        ValidationViolationException, InvalidConfigValueException, RecordDoesNotExistsForUpdateException, UnsupportedException {
		if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)) {
			throw new RecordDoesNotExistsForUpdateException(
			        "Record does not exits in '" + arangoDBSinkConfigBean.getTableName() + "' target table for update.");
		} else {
			NoSQLConstantsConfigBean constantsConfigBean;
			ArrayList<ExpressionEvaluator> expressionEvaluators;

			ArrayList<String> metaDataFields = null;
			if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)
			        && (arangoDBSinkGetResult.getResult() == null || arangoDBSinkGetResult.getResult().isEmpty())) {
				constantsConfigBean = arangoDBSinkConfigBean.getInsertConstantsConfigBean();
				expressionEvaluators = insertExpressionEvaluators;
			} else if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT) && arangoDBSinkGetResult.getResult() != null
			        && !arangoDBSinkGetResult.getResult().isEmpty()) {
				constantsConfigBean = arangoDBSinkConfigBean.getUpdateConstantsConfigBean();
				metaDataFields = arangoDBSinkConfigBean.getMetaDataFields();
				expressionEvaluators = updateExpressionEvaluators;
			} else {
				constantsConfigBean = arangoDBSinkConfigBean.getConstantsConfigBean();
				expressionEvaluators = this.expressionEvaluators;
			}

			HashMap<String, Object> record = ArangoDBSinkService.getRecord(arangoDBSinkConfigBean, row, arangoDBKey, constantsConfigBean, metaDataFields,
			        newStructure, expressionEvaluators);

			arangoDBBatchDataAndConnections.addInsertData(TaskContext.getPartitionId(), record);

			HashMap<String, Object> keys = getKeys(row);
			arangoDBBatchDataAndConnections.addKeysAdded(TaskContext.getPartitionId(), keys);

			return true;
		}
	}

	private boolean processRecordSelectedAndExists(HashMap<String, Object> row, String arangoDBKey, ArangoDBSinkGetResult arangoDBSinkGetResult)
	        throws RecordAlreadyExistsException, UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException,
	        DataCorruptedException, ValidationViolationException, InvalidConfigValueException {
		if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT)) {
			throw new RecordAlreadyExistsException("Record already exists.");
		} else if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT_IF_NOT_EXISTS)) {
			return false;
		} else {
			if (arangoDBSinkGetResult.isDoUpdate()) {
				HashMap<String, Object> record;

				if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
					record = ArangoDBSinkService.getRecord(arangoDBSinkConfigBean, row, arangoDBSinkGetResult.getKey(),
					        arangoDBSinkConfigBean.getUpdateConstantsConfigBean(), arangoDBSinkConfigBean.getMetaDataFields(), newStructure,
					        updateExpressionEvaluators);
				} else {
					record = ArangoDBSinkService.getRecord(arangoDBSinkConfigBean, row, arangoDBSinkGetResult.getKey(),
					        arangoDBSinkConfigBean.getConstantsConfigBean(), arangoDBSinkConfigBean.getMetaDataFields(), newStructure, expressionEvaluators);
				}

				arangoDBBatchDataAndConnections.addUpdateData(TaskContext.getPartitionId(), record);
				return true;
			} else {
				return false;
			}
		}
	}

	@Override
	public void beforeBatch(Iterator<HashMap<String, Object>> rows) throws DataCorruptedException, ValidationViolationException, RecordProcessingException {
		try {
			HashMap<String, HashMap<String, Object>> unselectedData = arangoDBBatchDataAndConnections.getUnSelectedArangoDBKeys(TaskContext.getPartitionId());
			if (unselectedData != null && !unselectedData.isEmpty()) {
				processUnselectedData(unselectedData);
			}

		} catch (DataCorruptedException | UnsupportedCoerceException | InvalidSituationException | RecordAlreadyExistsException
		        | RecordDoesNotExistsForUpdateException exception) {
			throw new DataCorruptedException(exception);
		} catch (ValidationViolationException | DateParseException | InvalidConfigValueException | ImproperValidationException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		} catch (Exception exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	private void processUnselectedData(HashMap<String, HashMap<String, Object>> unselectedData) throws UnsupportedCoerceException, InvalidSituationException,
	        DateParseException, ImproperValidationException, DataCorruptedException, ValidationViolationException, InvalidConfigValueException,
	        RecordAlreadyExistsException, RecordDoesNotExistsForUpdateException, UnsupportedException {
		processUnselectedDataExistsInDB(unselectedData);
		processUnselectedDataNotExistsInDB(unselectedData);
	}

	private void processUnselectedDataNotExistsInDB(HashMap<String, HashMap<String, Object>> unselectedData)
	        throws RecordDoesNotExistsForUpdateException, UnsupportedCoerceException, InvalidSituationException, DateParseException,
	        ImproperValidationException, DataCorruptedException, ValidationViolationException, InvalidConfigValueException, UnsupportedException {
		for (Entry<String, HashMap<String, Object>> unselectedDataRecord : unselectedData.entrySet()) {
			if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)) {
				throw new RecordDoesNotExistsForUpdateException(
				        "Record does not exits in '" + arangoDBSinkConfigBean.getTableName() + "' target table for update.");
			} else {
				HashMap<String, Object> record;

				if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
					record = ArangoDBSinkService.getRecord(arangoDBSinkConfigBean, unselectedDataRecord.getValue(), unselectedDataRecord.getKey(),
					        arangoDBSinkConfigBean.getInsertConstantsConfigBean(), null, newStructure, insertExpressionEvaluators);
				} else {
					record = ArangoDBSinkService.getRecord(arangoDBSinkConfigBean, unselectedDataRecord.getValue(), unselectedDataRecord.getKey(),
					        arangoDBSinkConfigBean.getConstantsConfigBean(), null, newStructure, expressionEvaluators);
				}

				arangoDBBatchDataAndConnections.addInsertData(TaskContext.getPartitionId(), record);
				HashMap<String, Object> keys = getKeys(unselectedDataRecord.getValue());

				arangoDBBatchDataAndConnections.addKeysAdded(TaskContext.getPartitionId(), keys);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void processUnselectedDataExistsInDB(HashMap<String, HashMap<String, Object>> unselectedData)
	        throws RecordAlreadyExistsException, UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException,
	        DataCorruptedException, ValidationViolationException, InvalidConfigValueException {

		ArangoCursor<HashMap> resultSet = arangoDBBatchDataAndConnections.getConnection(TaskContext.getPartitionId())
		        .db(arangoDBSinkConfigBean.getConnection().getDBName())
		        .query(ArangoDBSinkService.getFilterQuery(arangoDBSinkConfigBean.getTableName(), unselectedData.keySet().toArray()), null, null, HashMap.class);

		while (resultSet.hasNext()) {
			HashMap<String, Object> selectedRow = resultSet.next();

			if (selectedRow.containsKey(NOSQL.ARANGO_DB_KEY)) {
				String arangoDBKey = (String) selectedRow.remove(NOSQL.ARANGO_DB_KEY);
				HashMap<String, Object> row = unselectedData.remove(arangoDBKey);

				if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT)) {
					throw new RecordAlreadyExistsException("Record already exists.");
				} else if (!arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT_IF_NOT_EXISTS)) {
					HashMap<String, Object> record;

					if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
						record = ArangoDBSinkService.getRecord(arangoDBSinkConfigBean, row, arangoDBKey, arangoDBSinkConfigBean.getUpdateConstantsConfigBean(),
						        arangoDBSinkConfigBean.getMetaDataFields(), newStructure, updateExpressionEvaluators);
					} else {
						record = ArangoDBSinkService.getRecord(arangoDBSinkConfigBean, row, arangoDBKey, arangoDBSinkConfigBean.getConstantsConfigBean(),
						        arangoDBSinkConfigBean.getMetaDataFields(), newStructure, expressionEvaluators);
					}

					arangoDBBatchDataAndConnections.addUpdateData(TaskContext.getPartitionId(), record);
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public DBWrittenStats onBatch(Iterator<HashMap<String, Object>> rows, long batchNumber, long currentBatchCount,
	        ArrayList<HashMap<String, Object>> batchedRows) throws RecordProcessingException {
		try {
			long insertedCount = 0;
			long updatedCount = 0;
			long errorCount = 0;
			List insertData = arangoDBBatchDataAndConnections.getInsertData(TaskContext.getPartitionId());
			List updateData = arangoDBBatchDataAndConnections.getUpdateData(TaskContext.getPartitionId());

			ArangoDB arangoDBConnection = arangoDBBatchDataAndConnections.getConnection(TaskContext.getPartitionId());

			if (insertData != null) {
				DocumentImportOptions documentImportOptions = new DocumentImportOptions().details(true).complete(true);
				if (arangoDBSinkConfigBean.isWaitForSync()) {
					documentImportOptions = documentImportOptions.waitForSync(true);
				}

				if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT_IF_NOT_EXISTS)) {
					documentImportOptions.onDuplicate(DocumentImportOptions.OnDuplicate.ignore);
				} else if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT)) {
					documentImportOptions.onDuplicate(DocumentImportOptions.OnDuplicate.error);
				} else if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
					documentImportOptions.onDuplicate(DocumentImportOptions.OnDuplicate.update);
				}

				DocumentImportEntity documentImportEntity = arangoDBConnection.db(arangoDBSinkConfigBean.getConnection().getDBName())
				        .collection(arangoDBSinkConfigBean.getTableName()).importDocuments(insertData, documentImportOptions);
				if (documentImportEntity == null) {
					throw new DataCorruptedException("Unable to insert record into ArangoDB");
				} else {
					insertedCount = documentImportEntity.getCreated();
					updatedCount = documentImportEntity.getUpdated();
					errorCount = documentImportEntity.getErrors();

					if (errorCount > 0) {
						throw new DataCorruptedException("Unable to insert record into ArangoDB");
					}
				}
			}

			if (updateData != null) {
				if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)
				        || arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
					DocumentUpdateOptions documentUpdateOptions = new DocumentUpdateOptions().returnOld(true).returnNew(true);
					if (arangoDBSinkConfigBean.isWaitForSync()) {
						documentUpdateOptions = documentUpdateOptions.waitForSync(true);
					}

					MultiDocumentEntity<DocumentUpdateEntity<LinkedHashMap<String, Object>>> documentUpdateEntities = arangoDBConnection
					        .db(arangoDBSinkConfigBean.getConnection().getDBName()).collection(arangoDBSinkConfigBean.getTableName())
					        .updateDocuments(updateData, documentUpdateOptions);
					if (documentUpdateEntities == null) {
						throw new DataCorruptedException("Unable to update record into ArangoDB");
					} else {
						for (DocumentUpdateEntity<LinkedHashMap<String, Object>> documentUpdateEntity : documentUpdateEntities.getDocuments()) {
							boolean updated = ArangoDBSinkService.verifyUpdate(documentUpdateEntity.getOld(), documentUpdateEntity.getNew(),
							        arangoDBSinkConfigBean.getMetaDataFields(), newStructure);
							if (updated) {
								updatedCount += 1;
							}
						}

						errorCount = documentUpdateEntities.getErrors().size();
						if (errorCount > 0) {
							throw new DataCorruptedException("Unable to update record into ArangoDB. Details: "
							        + ((ErrorEntity) documentUpdateEntities.getErrors().toArray()[0]).getErrorMessage());
						}
					}
				}
			}

			return new DBWrittenStats(insertedCount, updatedCount, errorCount);
		} catch (Exception exception) {
			// TODO prepare data for revertion
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	@Override
	public void afterBatch(Iterator<HashMap<String, Object>> rows, ArrayList<HashMap<String, Object>> batchedRows) throws Exception {
		arangoDBBatchDataAndConnections.clearSelectData(TaskContext.getPartitionId());
		arangoDBBatchDataAndConnections.clearInsertData(TaskContext.getPartitionId());
		arangoDBBatchDataAndConnections.clearUpdateData(TaskContext.getPartitionId());
		arangoDBBatchDataAndConnections.clearUnSelectedArangoDBKeys(TaskContext.getPartitionId());
		arangoDBBatchDataAndConnections.clearKeysAdded(TaskContext.getPartitionId());
	}

	@Override
	public void onCallEnd(Iterator<HashMap<String, Object>> rows, BatchTrackingCountConfigBean trackingCounts) throws Exception {
	}

	@Override
	public void onBatchSuccess(long batchNumber, long currentBatchCount, ArrayList<HashMap<String, Object>> batchedRows) throws Exception {
	}

	@Override
	public void onBatchFail(long batchNumber, long currentBatchCount, ArrayList<HashMap<String, Object>> batchedRows) throws Exception {
		arangoDBBatchDataAndConnections.clearSelectData(TaskContext.getPartitionId());
		arangoDBBatchDataAndConnections.clearInsertData(TaskContext.getPartitionId());
		arangoDBBatchDataAndConnections.clearUpdateData(TaskContext.getPartitionId());
		arangoDBBatchDataAndConnections.clearUnSelectedArangoDBKeys(TaskContext.getPartitionId());
		arangoDBBatchDataAndConnections.clearKeysAdded(TaskContext.getPartitionId());
		// TODO get error records (not provided by ArangoDB)
	}

	@Override
	protected BaseAnvizentErrorSetter<HashMap<String, Object>, HashMap<String, Object>> getBaseAnvizentErrorRowSetter() throws InvalidArgumentsException {
		return AnvizentErrorSetter.getDefaultInstance();
	}
}

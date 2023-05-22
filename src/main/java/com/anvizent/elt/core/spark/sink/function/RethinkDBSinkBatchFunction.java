package com.anvizent.elt.core.spark.sink.function;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import com.anvizent.elt.core.listener.common.connection.RethinkDBConnectionByTaskId;
import com.anvizent.elt.core.spark.constant.Constants.NOSQL;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.constant.EmptyConstants;
import com.anvizent.elt.core.spark.exception.RecordAlreadyExistsException;
import com.anvizent.elt.core.spark.exception.RecordDoesNotExistsForUpdateException;
import com.anvizent.elt.core.spark.sink.config.bean.NoSQLConstantsConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.RethinkDBBatchDataAndConnections;
import com.anvizent.elt.core.spark.sink.config.bean.RethinkDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.service.RethinkDBOnConflictReplace;
import com.anvizent.elt.core.spark.sink.service.RethinkDBOnConflitIgnore;
import com.anvizent.elt.core.spark.sink.service.RethinkDBSinkService;
import com.anvizent.elt.core.spark.sink.util.bean.RethinkDBSinkGetResult;
import com.anvizent.elt.core.spark.util.CollectionUtil;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Insert;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

/**
 * @author Hareen Bejjanki
 *
 */
@SuppressWarnings({ "rawtypes" })
public class RethinkDBSinkBatchFunction extends BaseAnvizentBatchFunction<HashMap<String, Object>, HashMap<String, Object>> {
	private static final long serialVersionUID = 1L;

	private RethinkDBSinkConfigBean rethinkDBSinkConfigBean;
	private RethinkDBBatchDataAndConnections rethinkDBBatchDataAndConnections;
	private Table table;
	protected ArrayList<ExpressionEvaluator> expressionEvaluators;
	protected ArrayList<ExpressionEvaluator> insertExpressionEvaluators;
	protected ArrayList<ExpressionEvaluator> updateExpressionEvaluators;

	public RethinkDBSinkBatchFunction(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws InvalidRelationException, InvalidArgumentsException {
		super(rethinkDBSinkConfigBean, null, structure, newStructure, null, rethinkDBSinkConfigBean.getMaxRetryCount(), rethinkDBSinkConfigBean.getRetryDelay(),
		        rethinkDBSinkConfigBean.getRethinkInitRetryConfigBean().getMaxRetryCount(),
		        rethinkDBSinkConfigBean.getRethinkInitRetryConfigBean().getRetryDelay(),
		        rethinkDBSinkConfigBean.getRethinkDestroyRetryConfigBean().getMaxRetryCount(),
		        rethinkDBSinkConfigBean.getRethinkDestroyRetryConfigBean().getRetryDelay(), rethinkDBSinkConfigBean.getBatchSize(), accumulators,
		        errorHandlerSinkFunction, jobDetails);

		this.rethinkDBSinkConfigBean = rethinkDBSinkConfigBean;
		this.rethinkDBBatchDataAndConnections = new RethinkDBBatchDataAndConnections();
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

	@Override
	public void onCallStart(Iterator<HashMap<String, Object>> rows, BatchTrackingCountConfigBean trackingCounts) throws ImproperValidationException, Exception {
		if (rethinkDBBatchDataAndConnections.getConnection(TaskContext.getPartitionId()) == null) {
			rethinkDBBatchDataAndConnections.addConnection(TaskContext.getPartitionId(), getConnection());
		}
	}

	private Connection getConnection() throws ImproperValidationException, UnimplementedException, Exception {
		try {
			return (Connection) ApplicationConnectionBean.getInstance()
			        .get(new RethinkDBConnectionByTaskId(rethinkDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0];
		} catch (TimeoutException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	@Override
	public boolean process(HashMap<String, Object> row, ArrayList<HashMap<String, Object>> batchedRows)
	        throws DataCorruptedException, RecordProcessingException, ValidationViolationException {
		initExpressionEvaluators(rethinkDBSinkConfigBean.getConstantsConfigBean(), expressionEvaluators);
		initExpressionEvaluators(rethinkDBSinkConfigBean.getInsertConstantsConfigBean(), insertExpressionEvaluators);
		initExpressionEvaluators(rethinkDBSinkConfigBean.getUpdateConstantsConfigBean(), updateExpressionEvaluators);

		try {
			Object rId = RethinkDBSinkService.getOrGenerateId(rethinkDBSinkConfigBean, row);
			boolean canAvoidSelect = RethinkDBSinkService.canAvoidSelect(rethinkDBSinkConfigBean, rId);

			if (keysProcessed(rId, canAvoidSelect, row)) {
				return true;
			} else {
				return process(row, rId, canAvoidSelect);
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

	private boolean process(HashMap<String, Object> row, Object rId, boolean canAvoidSelect)
	        throws DataCorruptedException, ValidationViolationException, RecordDoesNotExistsForUpdateException, InvalidConfigValueException,
	        UnsupportedException, RecordProcessingException, TimeoutException, SQLException, RecordAlreadyExistsException {
		RethinkDBSinkGetResult rethinkDBSinkGetResult = RethinkDBSinkService.getRethinkDBSinkGetResult(rethinkDBSinkConfigBean, row, getTable(), rId,
		        canAvoidSelect);
		rethinkDBBatchDataAndConnections.addSelectData(TaskContext.getPartitionId(), rethinkDBSinkGetResult);

		if (rethinkDBSinkGetResult.getResult() != null) {
			return processRecordSelectedAndExists(row, rId);
		} else if (rId == null) {
			return processRecordNotExistsWithoutRId(row, rId);
		} else if (CollectionUtil.isEmpty(rethinkDBSinkConfigBean.getMetaDataFields())) {
			return processRecordUnselectedAndNoMetaDataFields(row, rId);
		} else {
			rethinkDBBatchDataAndConnections.addUnSelectedRIds(TaskContext.getPartitionId(), rId, row);
			return true;
		}
	}

	private boolean keysProcessed(Object rId, boolean canAvoidSelect, HashMap<String, Object> row)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, DataCorruptedException,
	        InvalidConfigValueException, UnsupportedException, ValidationViolationException {
		if (rId == null && !canAvoidSelect) {
			HashMap<String, Object> keys = getKeys(row);
			if (keys != null && rethinkDBBatchDataAndConnections.getKeysAdded(TaskContext.getPartitionId()) != null
			        && rethinkDBBatchDataAndConnections.getKeysAdded(TaskContext.getPartitionId()).contains(keys)) {
				MapObject rRow = RethinkDBSinkService.getRRecord(rethinkDBSinkConfigBean, row, rId,
				        rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT) ? rethinkDBSinkConfigBean.getUpdateConstantsConfigBean()
				                : rethinkDBSinkConfigBean.getConstantsConfigBean(),
				        expressionEvaluators);

				rethinkDBBatchDataAndConnections.addUpdatetData(TaskContext.getPartitionId(), rRow);
				return true;
			}
		}

		return false;
	}

	private boolean processRecordUnselectedAndNoMetaDataFields(HashMap<String, Object> row, Object rId)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, DataCorruptedException,
	        ValidationViolationException, InvalidConfigValueException {
		if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)) {
			MapObject rRow = RethinkDBSinkService.getRRecord(rethinkDBSinkConfigBean, row, rId, rethinkDBSinkConfigBean.getConstantsConfigBean(),
			        expressionEvaluators);

			rethinkDBBatchDataAndConnections.addUpdatetData(TaskContext.getPartitionId(), rRow);
		} else {
			MapObject rRow;
			if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
				rRow = RethinkDBSinkService.getRRecord(rethinkDBSinkConfigBean, row, rId, rethinkDBSinkConfigBean.getInsertConstantsConfigBean(),
				        insertExpressionEvaluators);
			} else {
				rRow = RethinkDBSinkService.getRRecord(rethinkDBSinkConfigBean, row, rId, rethinkDBSinkConfigBean.getConstantsConfigBean(),
				        expressionEvaluators);
			}

			rethinkDBBatchDataAndConnections.addInsertData(TaskContext.getPartitionId(), rRow);
		}

		return true;
	}

	private boolean processRecordNotExistsWithoutRId(HashMap<String, Object> row, Object rId)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, DataCorruptedException,
	        ValidationViolationException, InvalidConfigValueException, RecordDoesNotExistsForUpdateException, UnsupportedException {
		if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)) {
			throw new RecordDoesNotExistsForUpdateException(
			        "Record does not exits in '" + rethinkDBSinkConfigBean.getTableName() + "' target table for update.");
		} else {
			MapObject rRow;

			if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
				rRow = RethinkDBSinkService.getRRecord(rethinkDBSinkConfigBean, row, rId, rethinkDBSinkConfigBean.getUpdateConstantsConfigBean(),
				        updateExpressionEvaluators);
			} else {
				rRow = RethinkDBSinkService.getRRecord(rethinkDBSinkConfigBean, row, rId, rethinkDBSinkConfigBean.getConstantsConfigBean(),
				        expressionEvaluators);
			}

			rethinkDBBatchDataAndConnections.addInsertData(TaskContext.getPartitionId(), rRow);
			HashMap<String, Object> keys = getKeys(row);

			rethinkDBBatchDataAndConnections.addKeysAdded(TaskContext.getPartitionId(), keys);

			return true;
		}
	}

	private boolean processRecordSelectedAndExists(HashMap<String, Object> row, Object rId)
	        throws RecordAlreadyExistsException, UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException,
	        DataCorruptedException, ValidationViolationException, InvalidConfigValueException {
		if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT)) {
			throw new RecordAlreadyExistsException("Record already exists.");
		} else if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT_IF_NOT_EXISTS)) {
			return false;
		} else {
			MapObject rRow;
			if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
				rRow = RethinkDBSinkService.getRRecord(rethinkDBSinkConfigBean, row, rId, rethinkDBSinkConfigBean.getUpdateConstantsConfigBean(),
				        updateExpressionEvaluators);
			} else {
				rRow = RethinkDBSinkService.getRRecord(rethinkDBSinkConfigBean, row, rId, rethinkDBSinkConfigBean.getConstantsConfigBean(),
				        expressionEvaluators);
			}

			rethinkDBBatchDataAndConnections.addUpdatetData(TaskContext.getPartitionId(), rRow);
			return true;
		}
	}

	private HashMap<String, Object> getKeys(HashMap<String, Object> row) throws UnsupportedCoerceException, InvalidSituationException, DateParseException,
	        ImproperValidationException, InvalidConfigValueException, UnsupportedException, DataCorruptedException {
		if (rethinkDBSinkConfigBean.getKeyFields() == null || rethinkDBSinkConfigBean.getKeyFields().isEmpty()) {
			return null;
		} else {
			HashMap<String, Object> keys = new HashMap<>();

			for (int i = 0; i < rethinkDBSinkConfigBean.getKeyFields().size(); i++) {
				keys.put(rethinkDBSinkConfigBean.getKeyFields().get(i), row.get(rethinkDBSinkConfigBean.getKeyFields().get(i)));
			}

			return keys;
		}
	}

	@Override
	public void beforeBatch(Iterator<HashMap<String, Object>> rows) throws DataCorruptedException, ValidationViolationException, RecordProcessingException {
		try {
			HashMap<Object, HashMap<String, Object>> unselectedData = rethinkDBBatchDataAndConnections.getUnSelectedRIds(TaskContext.getPartitionId());
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

	private void processUnselectedData(HashMap<Object, HashMap<String, Object>> unselectedData) throws UnsupportedCoerceException, InvalidSituationException,
	        DateParseException, ImproperValidationException, DataCorruptedException, ValidationViolationException, InvalidConfigValueException,
	        RecordAlreadyExistsException, RecordDoesNotExistsForUpdateException, UnsupportedException {
		getTable();
		processUnselectedDataExistsInDB(unselectedData);
		processUnselectedDataNotExistsInDB(unselectedData);
	}

	private void processUnselectedDataNotExistsInDB(HashMap<Object, HashMap<String, Object>> unselectedData)
	        throws RecordDoesNotExistsForUpdateException, UnsupportedCoerceException, InvalidSituationException, DateParseException,
	        ImproperValidationException, DataCorruptedException, ValidationViolationException, InvalidConfigValueException, UnsupportedException {
		for (Entry<Object, HashMap<String, Object>> unselectedDataRecord : unselectedData.entrySet()) {
			if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)) {
				throw new RecordDoesNotExistsForUpdateException(
				        "Record does not exits in '" + rethinkDBSinkConfigBean.getTableName() + "' target table for update.");
			} else {
				MapObject rRow;
				if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
					rRow = RethinkDBSinkService.getRRecord(rethinkDBSinkConfigBean, unselectedDataRecord.getValue(), unselectedDataRecord.getKey(),
					        rethinkDBSinkConfigBean.getUpdateConstantsConfigBean(), updateExpressionEvaluators);
				} else {
					rRow = RethinkDBSinkService.getRRecord(rethinkDBSinkConfigBean, unselectedDataRecord.getValue(), unselectedDataRecord.getKey(),
					        rethinkDBSinkConfigBean.getConstantsConfigBean(), expressionEvaluators);
				}

				rethinkDBBatchDataAndConnections.addInsertData(TaskContext.getPartitionId(), rRow);
				HashMap<String, Object> keys = getKeys(unselectedDataRecord.getValue());

				rethinkDBBatchDataAndConnections.addKeysAdded(TaskContext.getPartitionId(), keys);
			}
		}
	}

	private void processUnselectedDataExistsInDB(HashMap<Object, HashMap<String, Object>> unselectedData)
	        throws RecordAlreadyExistsException, UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException,
	        DataCorruptedException, ValidationViolationException, InvalidConfigValueException {
		Cursor<? extends Map<String, Object>> resultSet = table.getAll(unselectedData.keySet().toArray())
		        .run(rethinkDBBatchDataAndConnections.getConnection(TaskContext.getPartitionId()));
		while (resultSet.hasNext()) {
			Map<String, Object> rSelectedRow = resultSet.next();
			if (rSelectedRow.containsKey(NOSQL.RETHINK_DB_ID)) {
				Object rId = rSelectedRow.remove(NOSQL.RETHINK_DB_ID);
				HashMap<String, Object> row = unselectedData.remove(rId);
				if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT)) {
					throw new RecordAlreadyExistsException("Record already exists.");
				} else if (!rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT_IF_NOT_EXISTS)) {
					MapObject rRow;

					if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
						rRow = RethinkDBSinkService.getRRecord(rethinkDBSinkConfigBean, row, rId, rethinkDBSinkConfigBean.getUpdateConstantsConfigBean(),
						        updateExpressionEvaluators);
					} else {
						rRow = RethinkDBSinkService.getRRecord(rethinkDBSinkConfigBean, row, rId, rethinkDBSinkConfigBean.getConstantsConfigBean(),
						        expressionEvaluators);
					}

					rethinkDBBatchDataAndConnections.addUpdatetData(TaskContext.getPartitionId(), rRow);
				}
			}
		}
	}

	@Override
	public DBWrittenStats onBatch(Iterator<HashMap<String, Object>> rows, long batchNumber, long currentBatchCount,
	        ArrayList<HashMap<String, Object>> batchedRows) throws RecordProcessingException {
		try {
			long insertedCount = 0;
			long updatedCount = 0;
			long errorCount = 0;
			List insertData = rethinkDBBatchDataAndConnections.getInsertData(TaskContext.getPartitionId());
			List updateData = rethinkDBBatchDataAndConnections.getUpdateData(TaskContext.getPartitionId());

			Connection connection = rethinkDBBatchDataAndConnections.getConnection(TaskContext.getPartitionId());
			getTable();

			if (insertData != null) {
				Insert insert = getTable().insert(insertData);
				if (rethinkDBSinkConfigBean.isSoftDurability()) {
					insert = insert.optArg(NOSQL.RethinkDB.DURABILITY, NOSQL.RethinkDB.SOFT);
				}

				if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT_IF_NOT_EXISTS)) {
					insert = insert.optArg(NOSQL.RethinkDB.CONFLICT, new RethinkDBOnConflitIgnore());
				} else if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
					insert = insert.optArg(NOSQL.RethinkDB.CONFLICT, new RethinkDBOnConflictReplace(rethinkDBSinkConfigBean));
				}

				Map result = insert.run(connection);

				if (result == null) {
					throw new DataCorruptedException("Unable to insert record into RethinkDB, details: " + RethinkDBSinkService.getErrorMessage(result));
				} else {
					insertedCount = (Long) result.get(NOSQL.RethinkDB.Response.INSERTED);
					updatedCount = (Long) result.get(NOSQL.RethinkDB.Response.REPLACED);
					errorCount = (Long) result.get(NOSQL.RethinkDB.Response.ERRORS);

					if (errorCount > 0) {
						throw new DataCorruptedException("Unable to insert record into RethinkDB, details: " + RethinkDBSinkService.getErrorMessage(result));
					}
				}
			}

			if (updateData != null) {
				Map result = table.update(updateData).run(connection);

				if (result == null) {
					throw new DataCorruptedException("Unable to insert record into RethinkDB, details: " + RethinkDBSinkService.getErrorMessage(result));
				} else {
					updatedCount += (Long) result.get(NOSQL.RethinkDB.Response.REPLACED);
					errorCount += (Long) result.get(NOSQL.RethinkDB.Response.ERRORS);

					if (errorCount > 0) {
						throw new DataCorruptedException("Unable to insert record into RethinkDB, details: " + RethinkDBSinkService.getErrorMessage(result));
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
		rethinkDBBatchDataAndConnections.clearSelectData(TaskContext.getPartitionId());
		rethinkDBBatchDataAndConnections.clearInsertData(TaskContext.getPartitionId());
		rethinkDBBatchDataAndConnections.clearUpdateData(TaskContext.getPartitionId());
		rethinkDBBatchDataAndConnections.clearUnSelectedRIds(TaskContext.getPartitionId());
		rethinkDBBatchDataAndConnections.clearKeysAdded(TaskContext.getPartitionId());
	}

	@Override
	public void onCallEnd(Iterator<HashMap<String, Object>> rows, BatchTrackingCountConfigBean trackingCounts) throws Exception {
	}

	@Override
	public void onBatchSuccess(long batchNumber, long currentBatchCount, ArrayList<HashMap<String, Object>> batchedRows) throws Exception {
	}

	@Override
	public void onBatchFail(long batchNumber, long currentBatchCount, ArrayList<HashMap<String, Object>> batchedRows) throws Exception {
		rethinkDBBatchDataAndConnections.clearSelectData(TaskContext.getPartitionId());
		rethinkDBBatchDataAndConnections.clearInsertData(TaskContext.getPartitionId());
		rethinkDBBatchDataAndConnections.clearUpdateData(TaskContext.getPartitionId());
		rethinkDBBatchDataAndConnections.clearUnSelectedRIds(TaskContext.getPartitionId());
		rethinkDBBatchDataAndConnections.clearKeysAdded(TaskContext.getPartitionId());
		// TODO get error records (not provided by RethinkDB)
	}

	private Table getTable() {
		if (table == null) {
			table = RethinkDB.r.table(rethinkDBSinkConfigBean.getTableName());
		}

		return table;
	}

	@Override
	protected BaseAnvizentErrorSetter<HashMap<String, Object>, HashMap<String, Object>> getBaseAnvizentErrorRowSetter() throws InvalidArgumentsException {
		return AnvizentErrorSetter.getDefaultInstance();
	}
}

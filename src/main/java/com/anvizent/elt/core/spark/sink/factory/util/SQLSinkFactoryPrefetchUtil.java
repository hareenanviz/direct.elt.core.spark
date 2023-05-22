package com.anvizent.elt.core.spark.sink.factory.util;

import static com.anvizent.elt.core.spark.constant.FunctionalConstant.TABLE_NAME;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnectionByTaskId;
import com.anvizent.elt.core.spark.common.util.QueryInfo;
import com.anvizent.elt.core.spark.common.util.SQLUtil;
import com.anvizent.elt.core.spark.constant.BatchType;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.constant.SQLUpdateType;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.sink.config.bean.DBConstantsConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;
import com.anvizent.elt.core.spark.sink.function.SQLSinkPrefetchUpsertBatchFunction;
import com.anvizent.elt.core.spark.sink.function.SQLSinkPrefetchUpsertBatchUsingTempTableFunction;
import com.anvizent.elt.core.spark.sink.function.bean.SQLSinkPrefetchAllUpsertFunctionBean;
import com.anvizent.elt.core.spark.sink.function.bean.SQLSinkPrefetchWithTempTableUpsertFunctionBean;
import com.anvizent.elt.core.spark.sink.service.SQLUpdateService;
import com.anvizent.elt.core.spark.util.StringUtil;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkFactoryPrefetchUtil {

	private SQLSinkConfigBean sqlSinkConfigBean;
	private LinkedHashMap<String, AnvizentDataType> structure;
	private JavaRDD<HashMap<String, Object>> rdd;

	public SQLSinkFactoryPrefetchUtil(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        JavaRDD<HashMap<String, Object>> rdd) {
		this.sqlSinkConfigBean = sqlSinkConfigBean;
		this.structure = structure;
		this.rdd = rdd;
	}

	public void write(ArrayList<AnvizentAccumulator> anvizentAccumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws ImproperValidationException, Exception {
		if (sqlSinkConfigBean.getBatchType().equals(BatchType.NONE)) {
			writeWithoutBatch(anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
		} else {
			writeByBatch(anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
		}
	}

	public void writeWithoutBatch(ArrayList<AnvizentAccumulator> anvizentAccumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws ImproperValidationException, Exception {
		// TODO
	}

	public void writeByBatch(ArrayList<AnvizentAccumulator> anvizentAccumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws ImproperValidationException, Exception {
		if (sqlSinkConfigBean.getUpdateUsing().equals(SQLUpdateType.TEMP_TABLE)) {
			writeByBatchUpdateUsingTempTable(anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
		} else if (sqlSinkConfigBean.getUpdateUsing().equals(SQLUpdateType.CSV)) {
			writeByBatchUpdateUsingCSV(anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
		} else {
			writeByBatchUpdateUsingBatch(anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
		}
	}

	public void writeByBatchUpdateUsingTempTable(ArrayList<AnvizentAccumulator> anvizentAccumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws ImproperValidationException, Exception {
		SQLSinkPrefetchWithTempTableUpsertFunctionBean functionBean = getTempTableFunctionBean();

		if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			rdd.foreachPartition(new SQLSinkPrefetchUpsertBatchUsingTempTableFunction(functionBean, sqlSinkConfigBean, structure, anvizentAccumulators,
			        errorHandlerSinkFunction, jobDetails));
		} else {
			// TODO
		}
	}

	public void writeByBatchUpdateUsingCSV(ArrayList<AnvizentAccumulator> anvizentAccumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws ImproperValidationException, Exception {
		// TODO
	}

	public void writeByBatchUpdateUsingBatch(ArrayList<AnvizentAccumulator> anvizentAccumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws ImproperValidationException, Exception {
		SQLSinkPrefetchAllUpsertFunctionBean functionBean = getFunctionBean();

		if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			rdd.foreachPartition(
			        new SQLSinkPrefetchUpsertBatchFunction(sqlSinkConfigBean, structure, anvizentAccumulators, errorHandlerSinkFunction, jobDetails));
		} else {
			// TODO
		}
	}

	private SQLSinkPrefetchWithTempTableUpsertFunctionBean getTempTableFunctionBean()
	        throws UnimplementedException, InvalidInputForConfigException, ImproperValidationException, ClassNotFoundException, SQLException, TimeoutException {
		SQLSinkPrefetchWithTempTableUpsertFunctionBean functionBean = new SQLSinkPrefetchWithTempTableUpsertFunctionBean(sqlSinkConfigBean, structure);

		// TODO
		ArrayList<String> fieldsFromStructure = new ArrayList<>(structure.keySet());
		ArrayList<String> columnsFromStructure = getColumnsFromStructure(fieldsFromStructure);

		functionBean.setInsertQuery(sqlSinkConfigBean.getInsertQuery());
		functionBean.setTempTableCreateQuery(getTempTableCreateQuery(columnsFromStructure, functionBean.getKeyColumns()));
		functionBean.setTempTableInsertQueryInfo(getTempTableInsertQuery(columnsFromStructure, fieldsFromStructure));
		functionBean.setUpdateQuery(getUpdateQueryUsingTempTable(functionBean.getTableName(), columnsFromStructure, functionBean.getKeyColumns(),
		        sqlSinkConfigBean.getUpdateConstantsConfigBean(), sqlSinkConfigBean.isKeyFieldsCaseSensitive()));

		return functionBean;
	}

	private SQLSinkPrefetchAllUpsertFunctionBean getFunctionBean() throws UnimplementedException, InvalidInputForConfigException {
		return new SQLSinkPrefetchAllUpsertFunctionBean(sqlSinkConfigBean, structure);
	}

	private ArrayList<String> getInsertFields() {
		LinkedHashMap<String, AnvizentDataType> structureAfterRemovingDeleteIndicator = new LinkedHashMap<String, AnvizentDataType>(structure);

		if (StringUtils.isNotBlank(sqlSinkConfigBean.getDeleteIndicatorField())) {
			structureAfterRemovingDeleteIndicator.remove(sqlSinkConfigBean.getDeleteIndicatorField());
		}

		return SQLUpdateService.getDifferFields(new ArrayList<>(structureAfterRemovingDeleteIndicator.keySet()), sqlSinkConfigBean.getFieldsDifferToColumns(),
		        sqlSinkConfigBean.getColumnsDifferToFields());
	}

	private QueryInfo getTempTableInsertQuery(ArrayList<String> columnsFromStructure, ArrayList<String> fieldsFromStructure) {
		QueryInfo queryInfo = new QueryInfo();

		queryInfo.setsToSet(fieldsFromStructure);

		String columns = "";
		String values = "";

		for (String column : columnsFromStructure) {
			if (!columns.isEmpty()) {
				columns += ", ";
				values += ", ";
			}

			columns += '`' + StringUtil.addMetaChar(column, '`', '\\') + '`';
			values += '?';
		}

		queryInfo.setQuery("INSERT INTO ${tableName}(" + columns + ") VALUES (" + values + ")");

		return queryInfo;
	}

	private String getUpdateQueryUsingTempTable(String tableName, ArrayList<String> columnsFromStructure, ArrayList<String> keyColumns,
	        DBConstantsConfigBean updateConstantsConfigBean, boolean caseSensitive) {
		String columns = "";

		for (String column : columnsFromStructure) {
			if (!columns.isEmpty()) {
				columns += ", ";
			}

			String afterQuote = '`' + StringUtil.addMetaChar(column, '`', '\\') + '`';
			columns += "t." + afterQuote + "=tt." + afterQuote;
		}

		String onCondition = "";
		for (String column : keyColumns) {
			if (!onCondition.isEmpty()) {
				onCondition += " AND ";
			}

			String afterQuote = '`' + StringUtil.addMetaChar(column, '`', '\\') + '`';

			if (caseSensitive) {
				onCondition += "BINARY ";
			}

			onCondition += "t." + afterQuote + "=tt." + afterQuote;
		}

		String constants = "";

		if (updateConstantsConfigBean != null) {
			int i = 0;
			for (String column : updateConstantsConfigBean.getColumns()) {
				String afterQuote = '`' + StringUtil.addMetaChar(column, '`', '\\') + '`';
				constants += ", t." + afterQuote + "=" + updateConstantsConfigBean.getValues().get(i++);
			}
		}

		return "UPDATE " + StringUtil.addMetaChar(tableName, '`', '\\') + " AS t JOIN ${tableName} AS tt ON " + onCondition + " SET " + columns + constants;
	}

	private String getTempTableCreateQuery(ArrayList<String> columns, ArrayList<String> keyColumns)
	        throws SQLException, ClassNotFoundException, ImproperValidationException, UnimplementedException, TimeoutException {
		SQLException sqlException = null;
		int i = 0;
		do {
			Connection connection = null;
			PreparedStatement statement = null;
			ResultSet rs = null;
			try {
				connection = (Connection) ApplicationConnectionBean.getInstance()
				        .get(new RDBMSConnectionByTaskId(sqlSinkConfigBean.getRdbmsConnection(), null, TaskContext.getPartitionId()), true)[0];

				String sql = "SHOW CREATE TABLE " + StringUtil.addMetaChar(sqlSinkConfigBean.getTableName(), '`', '\\');
				statement = connection.prepareStatement(sql);

				rs = statement.executeQuery();

				if (rs.next()) {
					String query = rs.getString(2);
					return getTempTableCreateStatment(sqlSinkConfigBean.getTableName(), query, columns, keyColumns);
				} else {
					throw new SQLSyntaxErrorException("Table with name: " + sqlSinkConfigBean.getTableName() + " not found");
				}
			} catch (SQLSyntaxErrorException exception) {
				throw exception;
			} catch (SQLException exception) {
				sqlException = exception;

				if (sqlSinkConfigBean.getRetryDelay() != null && sqlSinkConfigBean.getRetryDelay() > 0) {
					try {
						Thread.sleep(sqlSinkConfigBean.getRetryDelay());
					} catch (InterruptedException e) {
					}
				}
			} finally {
				SQLUtil.closeConnectionObjects(connection, statement, rs);
			}
		} while (++i < sqlSinkConfigBean.getMaxRetryCount());

		throw sqlException;
	}

	private String getTempTableCreateStatment(String tableName, String query, ArrayList<String> columns, ArrayList<String> keyColumns) {
		tableName = StringUtil.addMetaChar(tableName, '`', '\\');

		String newQuery = "";

		String queryHead = StringUtil.getQueryHead(tableName, query);
		queryHead = queryHead.replace('`' + tableName + '`', TABLE_NAME);

		int tailIndex = query.lastIndexOf(')');
		String queryTail = query.substring(tailIndex);

		for (String column : columns) {
			column = StringUtil.addMetaChar(column, '`', '\\');

			int startIndex = query.indexOf('`' + column + '`');
			int endIndex = query.indexOf('`', startIndex + ('`' + column + '`').length() + 2);

			String substring;
			if (endIndex == -1) {
				substring = query.substring(startIndex, tailIndex);
				int commaIndex = substring.lastIndexOf(',');
				if (commaIndex == -1) {
					newQuery += " " + substring;
				} else {
					int indexBefore = StringUtil.indexBefore(substring, commaIndex, '(');
					if (indexBefore == -1) {
						newQuery += " " + substring.substring(0, commaIndex + 1);
					} else {
						int closeBraseIndex = substring.indexOf(')', commaIndex);
						if (closeBraseIndex == -1) {
							newQuery += " " + substring.substring(0, commaIndex + 1);
						} else {
							newQuery += " " + substring;
						}
					}
				}
			} else {
				substring = query.substring(startIndex, endIndex);
				newQuery += " " + substring.substring(0, substring.lastIndexOf(',') + 1);
			}

			query.indexOf('(', 1);
		}

		newQuery = newQuery.trim();
//		if (newQuery.endsWith(",")) {
//			newQuery = newQuery.substring(0, newQuery.length() - 1);
//		}

		return queryHead + " " + newQuery + " PRIMARY KEY (" + StringUtil.join(keyColumns, ", ", '`', '\\') + ") " + " " + queryTail;
	}

	private ArrayList<String> getColumnsFromStructure(ArrayList<String> fieldsFromStructure) {
		ArrayList<String> fieldsDifferToColumns = sqlSinkConfigBean.getFieldsDifferToColumns();
		ArrayList<String> columnsDifferToFields = sqlSinkConfigBean.getColumnsDifferToFields();
		ArrayList<String> keyColumns = sqlSinkConfigBean.getKeyColumns();
		ArrayList<String> keyFields = sqlSinkConfigBean.getKeyFields();
		ArrayList<String> columnsFromStructure = new ArrayList<>();

		if ((columnsDifferToFields == null || fieldsDifferToColumns == null) && keyColumns == null) {
			columnsFromStructure = fieldsFromStructure;
		} else {
			if (columnsDifferToFields == null || fieldsDifferToColumns == null) {
				fieldsDifferToColumns = keyFields;
				columnsDifferToFields = keyColumns;
			} else if (keyColumns != null) {
				fieldsDifferToColumns.addAll(keyFields);
				columnsDifferToFields.addAll(keyColumns);
			}

			for (String field : fieldsFromStructure) {
				int index = fieldsDifferToColumns.indexOf(field);
				if (index != -1) {
					columnsFromStructure.add(columnsDifferToFields.get(index));
				} else {
					columnsFromStructure.add(field);
				}
			}
		}

		return columnsFromStructure;
	}
}

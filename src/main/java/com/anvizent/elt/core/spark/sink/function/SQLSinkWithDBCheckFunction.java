package com.anvizent.elt.core.spark.sink.function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.TaskContext;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.constant.Constants;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.stats.beans.DBWrittenRow;
import com.anvizent.elt.core.lib.stats.beans.WrittenRow;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.spark.common.util.SQLUtil;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.SQLUpsertWithDBCheckConnectionAndStatments;
import com.anvizent.elt.core.spark.sink.function.bean.SQLSinkDBQueryFunctionBean;
import com.anvizent.elt.core.spark.sink.function.util.SQLSinkWithDBCheckFunctionUtil;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkWithDBCheckFunction extends AnvizentVoidFunction {

	private static final long serialVersionUID = 1L;

	private SQLUpsertWithDBCheckConnectionAndStatments dbCheckStatements;
	private RDBMSConnection rdbmsConnection;
	private SQLSinkDBQueryFunctionBean functionBean;

	public SQLSinkWithDBCheckFunction(SQLSinkConfigBean sqlSinkConfigBean, SQLSinkDBQueryFunctionBean functionBean,
	        LinkedHashMap<String, AnvizentDataType> structure, ArrayList<AnvizentAccumulator> anvizentAccumulators,
	        AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails) throws ImproperValidationException, Exception {
		super(sqlSinkConfigBean, null, structure, structure, null, anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
		this.functionBean = functionBean;
		this.rdbmsConnection = sqlSinkConfigBean.getRdbmsConnection();
		this.dbCheckStatements = new SQLUpsertWithDBCheckConnectionAndStatments();
		SQLSinkWithDBCheckFunctionUtil.init(dbCheckStatements, rdbmsConnection, functionBean);
	}

	public void init() throws ImproperValidationException, Exception {
		try {
			if (dbCheckStatements.getConnection(TaskContext.getPartitionId()) == null
			        || dbCheckStatements.getConnection(TaskContext.getPartitionId()).isClosed()) {
				dbCheckStatements.addConnection(TaskContext.getPartitionId(), getConnection());
				SQLUtil.executeQuey(functionBean.getOnConnectRunQuery(), dbCheckStatements);

				dbCheckStatements.addUpsertStatement(TaskContext.getPartitionId(), getPreparedStatement(functionBean.getQueryInfo().getQuery()));

				if (functionBean.getDeleteQuery() != null && StringUtils.isNotBlank(functionBean.getDeleteQuery().getQuery())) {
					dbCheckStatements.addDeleteStatement(TaskContext.getPartitionId(), getPreparedStatement(functionBean.getDeleteQuery().getQuery()));
				}

				dbCheckStatements.getConnection(TaskContext.getPartitionId()).setAutoCommit(Boolean.FALSE);
			} else {
				if (dbCheckStatements.getUpsertStatement(TaskContext.getPartitionId()) == null
				        || dbCheckStatements.getUpsertStatement(TaskContext.getPartitionId()).isClosed()) {
					dbCheckStatements.addUpsertStatement(TaskContext.getPartitionId(), getPreparedStatement(functionBean.getQueryInfo().getQuery()));
				}

				if (functionBean.getDeleteQuery() != null && StringUtils.isNotBlank(functionBean.getDeleteQuery().getQuery())
				        && (dbCheckStatements.getDeleteStatement(TaskContext.getPartitionId()) == null
				                || dbCheckStatements.getDeleteStatement(TaskContext.getPartitionId()).isClosed())) {
					dbCheckStatements.addDeleteStatement(TaskContext.getPartitionId(), getPreparedStatement(functionBean.getDeleteQuery().getQuery()));
				}
			}
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	@Override
	public WrittenRow process(HashMap<String, Object> row) throws RecordProcessingException, ValidationViolationException {
		try {
			return upsert(row);
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		} catch (ImproperValidationException | UnimplementedException | TimeoutException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		}
	}

	private WrittenRow upsert(HashMap<String, Object> row) throws SQLException, ImproperValidationException, UnimplementedException, TimeoutException {
		String deleteField = functionBean.getDeleteIndicatorField();

		if (StringUtils.isNotBlank(deleteField) && row.get(deleteField) != null && (Boolean) row.get(deleteField)) {
			PreparedStatement statement = dbCheckStatements.getDeleteStatement(TaskContext.getPartitionId());
			SQLUtil.setPreparedStatement(statement, row, functionBean.getDeleteQuery().getFieldsToSet());
			statement.executeUpdate();
		} else {
			PreparedStatement statement = dbCheckStatements.getUpsertStatement(TaskContext.getPartitionId());
			SQLUtil.setPreparedStatement(statement, row, functionBean.getQueryInfo().getMultiValuedFieldIndexesToSet());

			int insert = statement.executeUpdate();
			if (insert <= 0) {
				throw new SQLException(ExceptionMessage.FAILED_TO_INSERT);
			}
		}

		return new DBWrittenRow(newStructure, row, row, false);
	}

	private PreparedStatement getPreparedStatement(String query) throws SQLException {
		return query == null ? null : dbCheckStatements.getConnection(TaskContext.getPartitionId()).prepareStatement(query);
	}

	private Connection getConnection() throws SQLException, ImproperValidationException {
		try {
			Class.forName(rdbmsConnection.getDriver());

			String jdbcURL = rdbmsConnection.getJdbcURL();
			if (rdbmsConnection.getDriver().equals(Constants.MYSQL_DRIVER)) {
				jdbcURL = rdbmsConnection.checkAndAddParam(jdbcURL, "rewriteBatchedStatements", General.REWRITE_BATCHED_STATEMENTS, "?", "&");
			}

			return DriverManager.getConnection(jdbcURL, rdbmsConnection.getUserName(), rdbmsConnection.getPassword());
		} catch (ClassNotFoundException exception) {
			throw new ImproperValidationException(exception);
		}
	}
}

package com.anvizent.elt.core.spark.sink.function.util;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.TaskContext;

import com.anvizent.elt.core.lib.constant.Constants;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.spark.common.util.SQLUtil;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.sink.config.bean.SQLUpsertWithDBCheckConnectionAndStatments;
import com.anvizent.elt.core.spark.sink.function.bean.SQLSinkDBQueryFunctionBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkWithDBCheckFunctionUtil implements Serializable {
	private static final long serialVersionUID = 1L;

	public static void init(SQLUpsertWithDBCheckConnectionAndStatments dbCheckStatements, RDBMSConnection rdbmsConnection,
	        SQLSinkDBQueryFunctionBean functionBean) throws ImproperValidationException, Exception {
		try {
			if (dbCheckStatements.getConnection(TaskContext.getPartitionId()) == null
			        || dbCheckStatements.getConnection(TaskContext.getPartitionId()).isClosed()) {
				dbCheckStatements.addConnection(TaskContext.getPartitionId(), getConnection(rdbmsConnection, dbCheckStatements));
				SQLUtil.executeQuey(functionBean.getOnConnectRunQuery(), dbCheckStatements);

				dbCheckStatements.addUpsertStatement(TaskContext.getPartitionId(),
				        getPreparedStatement(dbCheckStatements, functionBean.getQueryInfo().getQuery()));

				if (functionBean.getDeleteQuery() != null && StringUtils.isNotBlank(functionBean.getDeleteQuery().getQuery())) {
					dbCheckStatements.addDeleteStatement(TaskContext.getPartitionId(),
					        getPreparedStatement(dbCheckStatements, functionBean.getDeleteQuery().getQuery()));
				}

				dbCheckStatements.getConnection(TaskContext.getPartitionId()).setAutoCommit(Boolean.FALSE);
			} else {
				if (dbCheckStatements.getUpsertStatement(TaskContext.getPartitionId()) == null
				        || dbCheckStatements.getUpsertStatement(TaskContext.getPartitionId()).isClosed()) {
					dbCheckStatements.addUpsertStatement(TaskContext.getPartitionId(),
					        getPreparedStatement(dbCheckStatements, functionBean.getQueryInfo().getQuery()));
				}

				if (functionBean.getDeleteQuery() != null && StringUtils.isNotBlank(functionBean.getDeleteQuery().getQuery())
				        && (dbCheckStatements.getDeleteStatement(TaskContext.getPartitionId()) == null
				                || dbCheckStatements.getDeleteStatement(TaskContext.getPartitionId()).isClosed())) {
					dbCheckStatements.addDeleteStatement(TaskContext.getPartitionId(),
					        getPreparedStatement(dbCheckStatements, functionBean.getDeleteQuery().getQuery()));
				}
			}
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	private static PreparedStatement getPreparedStatement(SQLUpsertWithDBCheckConnectionAndStatments dbCheckStatements, String query) throws SQLException {
		return query == null ? null : dbCheckStatements.getConnection(TaskContext.getPartitionId()).prepareStatement(query);
	}

	private static Connection getConnection(RDBMSConnection rdbmsConnection, SQLUpsertWithDBCheckConnectionAndStatments dbCheckStatements)
	        throws SQLException, ImproperValidationException {
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

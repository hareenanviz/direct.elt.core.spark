package com.anvizent.elt.core.spark.sink.config.bean;

import java.sql.PreparedStatement;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SQLUpsertBatchWithTempTableConnections extends SQLConnectionByPartition {

	private static final long serialVersionUID = 1L;

	private HashMap<Integer, PreparedStatement> insertStatements = new HashMap<>();
	private HashMap<Integer, PreparedStatement> tempTableInsertStatements = new HashMap<>();
	private HashMap<Integer, PreparedStatement> deleteStatements = new HashMap<>();
	private HashMap<Integer, String> tempTableNames = new HashMap<>();
	private HashMap<Integer, String> updateQueries = new HashMap<>();

	public void addInsertStatement(int partitionId, PreparedStatement preparedStatement) {
		if (preparedStatement != null) {
			this.insertStatements.put(partitionId, preparedStatement);
		}
	}

	public void addTempTableInsertStatement(int partitionId, PreparedStatement preparedStatement) {
		if (preparedStatement != null) {
			this.tempTableInsertStatements.put(partitionId, preparedStatement);
		}
	}

	public void addDeleteStatement(int partitionId, PreparedStatement preparedStatement) {
		if (preparedStatement != null) {
			this.deleteStatements.put(partitionId, preparedStatement);
		}
	}

	public void addTempTableNames(int partitionId, String tableName) {
		if (StringUtils.isNotBlank(tableName)) {
			this.tempTableNames.put(partitionId, tableName);
		}
	}

	public void addUpdateQueries(int partitionId, String updateQuery) {
		if (StringUtils.isNotBlank(updateQuery)) {
			this.updateQueries.put(partitionId, updateQuery);
		}
	}

	public void removeTempTableNames(int partitionId) {
		this.tempTableNames.remove(partitionId);
	}

	public PreparedStatement getInsertStatement(int paritionId) {
		return this.insertStatements.get(paritionId);
	}

	public PreparedStatement getTempTableInsertStatement(int paritionId) {
		return this.tempTableInsertStatements.get(paritionId);
	}

	public PreparedStatement getDeleteStatement(int paritionId) {
		return this.deleteStatements.get(paritionId);
	}

	public String getTempTableName(int paritionId) {
		return this.tempTableNames.get(paritionId);
	}

	public String getUpdateQuery(int paritionId) {
		return this.updateQueries.get(paritionId);
	}

	@Override
	public String toString() {
		return "SQLUpsertBatchConnections [insertStatements=" + insertStatements + ", tempTableInsertStatements=" + tempTableInsertStatements
		        + ", deleteStatements=" + deleteStatements + ", tempTableNames=" + tempTableNames + ", updateQueries=" + updateQueries + "]";
	}

}
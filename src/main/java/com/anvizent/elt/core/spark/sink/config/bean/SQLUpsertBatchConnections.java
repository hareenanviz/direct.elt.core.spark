package com.anvizent.elt.core.spark.sink.config.bean;

import java.sql.PreparedStatement;
import java.util.HashMap;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SQLUpsertBatchConnections extends SQLConnectionByPartition {

	private static final long serialVersionUID = 1L;

	private HashMap<Integer, PreparedStatement> insertStatements = new HashMap<>();
	private HashMap<Integer, PreparedStatement> updateStatements = new HashMap<>();
	private HashMap<Integer, PreparedStatement> deleteStatements = new HashMap<>();

	public void addInsertStatement(int partitionId, PreparedStatement preparedStatement) {
		if (preparedStatement != null) {
			this.insertStatements.put(partitionId, preparedStatement);
		}
	}

	public void addUpdateStatement(int partitionId, PreparedStatement preparedStatement) {
		if (preparedStatement != null) {
			this.updateStatements.put(partitionId, preparedStatement);
		}
	}

	public void addDeleteStatement(int partitionId, PreparedStatement preparedStatement) {
		if (preparedStatement != null) {
			this.deleteStatements.put(partitionId, preparedStatement);
		}
	}

	public PreparedStatement getInsertStatement(int paritionId) {
		return this.insertStatements.get(paritionId);
	}

	public PreparedStatement getUpdateStatement(int paritionId) {
		return this.updateStatements.get(paritionId);
	}

	public PreparedStatement getDeleteStatement(int paritionId) {
		return this.deleteStatements.get(paritionId);
	}

	public HashMap<Integer, PreparedStatement> getDeleteStatements() {
		return deleteStatements;
	}

	@Override
	public String toString() {
		return "SQLUpsertBatchConnections [insertStatements=" + insertStatements + ", updateStatements=" + updateStatements + ", deleteStatements="
		        + deleteStatements + "]";
	}

}
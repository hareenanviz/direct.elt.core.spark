package com.anvizent.elt.core.spark.sink.config.bean;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SQLInsertBatchConnections implements Serializable {

	private static final long serialVersionUID = 1L;

	private HashMap<Integer, Connection> connections = new HashMap<>();
	private HashMap<Integer, PreparedStatement> selectStatements = new HashMap<>();
	private HashMap<Integer, PreparedStatement> insertStatements = new HashMap<>();
	private HashMap<Integer, PreparedStatement> deleteStatements = new HashMap<>();

	public void addConnection(int partitionId, Connection connection) {
		this.connections.put(partitionId, connection);
	}

	public void addSelectStatement(int partitionId, PreparedStatement preparedStatement) {
		if (preparedStatement != null) {
			this.selectStatements.put(partitionId, preparedStatement);
		}
	}

	public void addInsertStatement(int partitionId, PreparedStatement preparedStatement) {
		if (preparedStatement != null) {
			this.insertStatements.put(partitionId, preparedStatement);
		}
	}

	public void addDeleteStatement(int partitionId, PreparedStatement preparedStatement) {
		if (preparedStatement != null) {
			this.deleteStatements.put(partitionId, preparedStatement);
		}
	}

	public Connection getConnection(int paritionId) {
		return this.connections.get(paritionId);
	}

	public PreparedStatement getSelectStatement(int paritionId) {
		return this.selectStatements.get(paritionId);
	}

	public PreparedStatement getInsertStatement(int paritionId) {
		return this.insertStatements.get(paritionId);
	}

	public PreparedStatement getDeleteStatement(int paritionId) {
		return this.deleteStatements.get(paritionId);
	}

	public HashMap<Integer, PreparedStatement> getDeleteStatements() {
		return deleteStatements;
	}
}

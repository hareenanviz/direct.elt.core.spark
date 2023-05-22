package com.anvizent.elt.core.spark.sink.config.bean;

import java.sql.PreparedStatement;
import java.util.HashMap;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLUpsertWithDBCheckConnectionAndStatments extends SQLConnectionByPartition {

	private static final long serialVersionUID = 1L;

	private HashMap<Integer, PreparedStatement> upsertStatements = new HashMap<>();
	private HashMap<Integer, PreparedStatement> deleteStatements = new HashMap<>();

	public void addUpsertStatement(int partitionId, PreparedStatement preparedStatement) {
		if (preparedStatement != null) {
			this.upsertStatements.put(partitionId, preparedStatement);
		}
	}

	public void addDeleteStatement(int partitionId, PreparedStatement preparedStatement) {
		if (preparedStatement != null) {
			this.deleteStatements.put(partitionId, preparedStatement);
		}
	}

	public PreparedStatement getUpsertStatement(int paritionId) {
		return this.upsertStatements.get(paritionId);
	}

	public PreparedStatement getDeleteStatement(int paritionId) {
		return this.deleteStatements.get(paritionId);
	}
}

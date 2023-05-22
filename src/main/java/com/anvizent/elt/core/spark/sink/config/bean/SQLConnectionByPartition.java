package com.anvizent.elt.core.spark.sink.config.bean;

import java.io.Serializable;
import java.sql.Connection;
import java.util.HashMap;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class SQLConnectionByPartition implements Serializable {

	private static final long serialVersionUID = 1L;

	private HashMap<Integer, Connection> connections = new HashMap<>();

	public void addConnection(int partitionId, Connection connection) {
		this.connections.put(partitionId, connection);
	}

	public Connection getConnection(int paritionId) {
		return this.connections.get(paritionId);
	}
}

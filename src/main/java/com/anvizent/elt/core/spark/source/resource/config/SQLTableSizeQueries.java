package com.anvizent.elt.core.spark.source.resource.config;

import java.io.Serializable;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLTableSizeQueries implements Serializable {
	private static final long serialVersionUID = 1L;

	private String table;
	private String query;

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

}

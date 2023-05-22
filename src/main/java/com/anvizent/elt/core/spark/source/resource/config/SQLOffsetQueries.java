package com.anvizent.elt.core.spark.source.resource.config;

import java.io.Serializable;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLOffsetQueries implements Serializable {
	private static final long serialVersionUID = 1L;

	private SQLOffsetQuery table;
	private SQLOffsetQuery queryWithoutOffSet;
	private SQLOffsetQuery queryWithOffSet;

	public SQLOffsetQuery getTable() {
		return table;
	}

	public void setTable(SQLOffsetQuery table) {
		this.table = table;
	}

	public SQLOffsetQuery getQueryWithoutOffSet() {
		return queryWithoutOffSet;
	}

	public void setQueryWithoutOffSet(SQLOffsetQuery queryWithoutOffSet) {
		this.queryWithoutOffSet = queryWithoutOffSet;
	}

	public SQLOffsetQuery getQueryWithOffSet() {
		return queryWithOffSet;
	}

	public void setQueryWithOffSet(SQLOffsetQuery queryWithOffSet) {
		this.queryWithOffSet = queryWithOffSet;
	}

}

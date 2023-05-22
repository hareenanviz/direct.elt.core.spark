package com.anvizent.elt.core.spark.sink.function.bean;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.spark.common.util.QueryInfo;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkPrefetchWithTempTableUpsertFunctionBean extends SQLSinkPrefetchFunctionBean {
	private static final long serialVersionUID = 1L;

	private String insertQuery;
	private String updateQuery;
	private String tempTableCreateQuery;
	private QueryInfo tempTableInsertQueryInfo;

	public String getInsertQuery() {
		return insertQuery;
	}

	public void setInsertQuery(String insertQuery) {
		this.insertQuery = insertQuery;
	}

	public String getUpdateQuery() {
		return updateQuery;
	}

	public void setUpdateQuery(String updateQuery) {
		this.updateQuery = updateQuery;
	}

	public String getTempTableCreateQuery() {
		return tempTableCreateQuery;
	}

	public void setTempTableCreateQuery(String tempTableCreateQuery) {
		this.tempTableCreateQuery = tempTableCreateQuery;
	}

	public QueryInfo getTempTableInsertQueryInfo() {
		return tempTableInsertQueryInfo;
	}

	public void setTempTableInsertQueryInfo(QueryInfo tempTableInsertQueryInfo) {
		this.tempTableInsertQueryInfo = tempTableInsertQueryInfo;
	}

	public SQLSinkPrefetchWithTempTableUpsertFunctionBean(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure) {
		super(sqlSinkConfigBean, structure);
	}
}
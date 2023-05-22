package com.anvizent.elt.core.spark.sink.function.bean;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.spark.common.util.QueryInfo;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkPrefetchAllUpsertFunctionBean extends SQLSinkPrefetchFunctionBean {
	private static final long serialVersionUID = 1L;

	private QueryInfo insertQueryInfo;
	private QueryInfo updateQueryInfo;

	public QueryInfo getInsertQueryInfo() {
		return insertQueryInfo;
	}

	public void setInsertQueryInfo(QueryInfo insertQueryInfo) {
		this.insertQueryInfo = insertQueryInfo;
	}

	public QueryInfo getUpdateQueryInfo() {
		return updateQueryInfo;
	}

	public void setUpdateQueryInfo(QueryInfo updateQueryInfo) {
		this.updateQueryInfo = updateQueryInfo;
	}

	public SQLSinkPrefetchAllUpsertFunctionBean(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure) {
		super(sqlSinkConfigBean, structure);
	}
}
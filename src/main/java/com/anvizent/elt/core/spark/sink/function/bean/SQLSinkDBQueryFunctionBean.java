package com.anvizent.elt.core.spark.sink.function.bean;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.spark.common.util.QueryInfo;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkDBQueryFunctionBean extends SQLSinkFunctionBean {

	private static final long serialVersionUID = 1L;

	private QueryInfo queryInfo;

	public QueryInfo getQueryInfo() {
		return queryInfo;
	}

	public void setQueryInfo(QueryInfo queryInfo) {
		this.queryInfo = queryInfo;
	}

	public SQLSinkDBQueryFunctionBean(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure) {
		super(sqlSinkConfigBean, structure);
	}
}
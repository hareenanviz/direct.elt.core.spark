package com.anvizent.elt.core.spark.sink.function.bean;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.commons.collections4.CollectionUtils;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.FunctionBean;
import com.anvizent.elt.core.lib.constant.StoreType;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.spark.common.util.QueryInfo;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;
import com.anvizent.elt.core.spark.sink.service.SQLSinkService;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkFunctionBean extends FunctionBean {

	private static final long serialVersionUID = 1L;

	private StoreType storeType;
	private String tableName;
	private QueryInfo deleteQuery;
	private String onConnectRunQuery;
	private String deleteIndicatorField;
	private ArrayList<String> keyColumns;
	private ArrayList<String> keyFields;
	private RDBMSConnection rdbmsConnection;
	private boolean keyFieldsCaseSensitive;
	private boolean alwaysUpdate;
	private ArrayList<String> selectColumns;
	private ArrayList<String> selectFields;
	private ArrayList<String> rowKeysAndFields;

	public StoreType getStoreType() {
		return storeType;
	}

	public void setStoreType(StoreType storeType) {
		this.storeType = storeType;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public QueryInfo getDeleteQuery() {
		return deleteQuery;
	}

	public void setDeleteQuery(QueryInfo deleteQuery) {
		this.deleteQuery = deleteQuery;
	}

	public String getOnConnectRunQuery() {
		return onConnectRunQuery;
	}

	public void setOnConnectRunQuery(String onConnectRunQuery) {
		this.onConnectRunQuery = onConnectRunQuery;
	}

	public String getDeleteIndicatorField() {
		return deleteIndicatorField;
	}

	public void setDeleteIndicatorField(String deleteIndicatorField) {
		this.deleteIndicatorField = deleteIndicatorField;
	}

	public ArrayList<String> getKeyColumns() {
		return keyColumns;
	}

	public void setKeyColumns(ArrayList<String> keyColumns) {
		this.keyColumns = keyColumns;
	}

	public ArrayList<String> getKeyFields() {
		return keyFields;
	}

	public void setKeyFields(ArrayList<String> keyFields) {
		this.keyFields = keyFields;
	}

	public RDBMSConnection getRdbmsConnection() {
		return rdbmsConnection;
	}

	public void setRdbmsConnection(RDBMSConnection rdbmsConnection) {
		this.rdbmsConnection = rdbmsConnection;
	}

	public boolean isKeyFieldsCaseSensitive() {
		return keyFieldsCaseSensitive;
	}

	public void setKeyFieldsCaseSensitive(boolean keyFieldsCaseSensitive) {
		this.keyFieldsCaseSensitive = keyFieldsCaseSensitive;
	}

	public boolean isAlwaysUpdate() {
		return alwaysUpdate;
	}

	public void setAlwaysUpdate(boolean alwaysUpdate) {
		this.alwaysUpdate = alwaysUpdate;
	}

	public ArrayList<String> getSelectColumns() {
		return selectColumns;
	}

	public void setSelectColumns(ArrayList<String> selectColumns) {
		this.selectColumns = selectColumns;
	}

	public ArrayList<String> getSelectFields() {
		return selectFields;
	}

	public void setSelectFields(ArrayList<String> selectFields) {
		this.selectFields = selectFields;
	}

	public ArrayList<String> getRowKeysAndFields() {
		return rowKeysAndFields;
	}

	public void setRowKeysAndFields(ArrayList<String> rowKeysAndFields) {
		this.rowKeysAndFields = rowKeysAndFields;
	}

	public SQLSinkFunctionBean(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure) {
		super(sqlSinkConfigBean);

		this.storeType = StoreType.getInstance(sqlSinkConfigBean.getRdbmsConnection().getDriver());
		this.tableName = sqlSinkConfigBean.getTableName();
		this.deleteQuery = SQLSinkService.buildDeleteQueries(structure.keySet(), sqlSinkConfigBean);
		this.onConnectRunQuery = sqlSinkConfigBean.getOnConnectRunQuery();
		this.deleteIndicatorField = sqlSinkConfigBean.getDeleteIndicatorField();
		this.keyFields = sqlSinkConfigBean.getKeyFields();

		if (CollectionUtils.isNotEmpty(sqlSinkConfigBean.getKeyColumns())) {
			this.keyColumns = sqlSinkConfigBean.getKeyColumns();
		} else {
			this.keyColumns = this.keyFields;
		}

		this.rdbmsConnection = sqlSinkConfigBean.getRdbmsConnection();
		this.keyFieldsCaseSensitive = sqlSinkConfigBean.isKeyFieldsCaseSensitive();
		this.alwaysUpdate = sqlSinkConfigBean.isAlwaysUpdate();
		this.selectColumns = sqlSinkConfigBean.getSelectColumns();
		this.selectFields = sqlSinkConfigBean.getSelectFields();
		this.rowKeysAndFields = sqlSinkConfigBean.getRowKeysAndFields();
	}
}

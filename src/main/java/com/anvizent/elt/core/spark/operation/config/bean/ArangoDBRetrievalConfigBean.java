package com.anvizent.elt.core.spark.operation.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.RetryMandatoryConfigBean;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.connection.ArangoDBConnection;
import com.anvizent.elt.core.spark.constant.CacheMode;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ArangoDBRetrievalConfigBean extends ConfigBean implements SimpleOperationConfigBean, RetryMandatoryConfigBean {

	private static final long serialVersionUID = 1L;

	private String tableName;
	private ArrayList<String> selectFields;
	private ArrayList<AnvizentDataType> selectFieldTypes;
	private ArrayList<String> selectFieldDateFormats;
	private ArrayList<Integer> selectFieldPositions;
	private ArrayList<String> selectFieldAliases;
	private ArrayList<String> whereFields;
	private ArrayList<String> whereColumns;
	private String customWhere;
	private ArrayList<String> insertValues;
	private OnZeroFetchOperation onZeroFetch;
	private ArrayList<String> orderByFields;
	private String orderByType;
	private Integer maxElementsInMemory;
	private Long timeToIdleSeconds;
	private CacheType cacheType;
	private CacheMode cacheMode;
	private ArangoDBConnection arangoDBConnection;
	private String insertQuery;
	private ArrayList<Object> rowInsertValues;
	private ArrayList<Object> collectionInsertValues;
	private boolean waitForSync;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public ArrayList<String> getSelectFields() {
		return selectFields;
	}

	public void setSelectFields(ArrayList<String> selectFields) {
		this.selectFields = selectFields;
	}

	public void setStructure(ArrayList<String> selectFields, ArrayList<Class<?>> selectFieldDataTypes) throws UnsupportedException {
		if (selectFields != null && selectFieldDataTypes != null) {
			this.selectFieldTypes = new ArrayList<>();

			for (int i = 0; i < selectFieldDataTypes.size(); i++) {
				Class<?> type = selectFieldDataTypes.get(i);
				this.selectFieldTypes.add(new AnvizentDataType(type));
			}
		} else {
			this.selectFieldTypes = null;
		}
	}

	public ArrayList<AnvizentDataType> getSelectFieldTypes() {
		return selectFieldTypes;
	}

	public void setSelectFieldTypes(ArrayList<AnvizentDataType> selectFieldTypes) {
		this.selectFieldTypes = selectFieldTypes;
	}

	public ArrayList<String> getSelectFieldDateFormats() {
		return selectFieldDateFormats;
	}

	public void setSelectFieldDateFormats(ArrayList<String> selectFieldDateFormats) {
		this.selectFieldDateFormats = selectFieldDateFormats;
	}

	public ArrayList<Integer> getSelectFieldPositions() {
		return selectFieldPositions;
	}

	public void setSelectFieldPositions(ArrayList<Integer> selectFieldPositions) {
		this.selectFieldPositions = selectFieldPositions;
	}

	public ArrayList<String> getSelectFieldAliases() {
		return selectFieldAliases;
	}

	public void setSelectFieldAliases(ArrayList<String> selectFieldAliases) {
		this.selectFieldAliases = selectFieldAliases;
	}

	public ArrayList<String> getWhereFields() {
		return whereFields;
	}

	public void setWhereFields(ArrayList<String> whereFields) {
		this.whereFields = whereFields;
	}

	public ArrayList<String> getWhereColumns() {
		return whereColumns;
	}

	public void setWhereColumns(ArrayList<String> whereColumns) {
		this.whereColumns = whereColumns;
	}

	public String getCustomWhere() {
		return customWhere;
	}

	public void setCustomWhere(String customWhere) {
		this.customWhere = customWhere;
	}

	public ArrayList<String> getInsertValues() {
		return insertValues;
	}

	public void setInsertValues(ArrayList<String> insertValues) {
		this.insertValues = insertValues;
	}

	public OnZeroFetchOperation getOnZeroFetch() {
		return onZeroFetch;
	}

	public void setOnZeroFetch(OnZeroFetchOperation onZeroFetch) {
		this.onZeroFetch = onZeroFetch;
	}

	public ArrayList<String> getOrderByFields() {
		return orderByFields;
	}

	public void setOrderByFields(ArrayList<String> orderByFields) {
		this.orderByFields = orderByFields;
	}

	public String getOrderByType() {
		return orderByType;
	}

	public void setOrderByType(String orderByType) {
		this.orderByType = orderByType;
	}

	public Integer getMaxElementsInMemory() {
		return maxElementsInMemory;
	}

	public void setMaxElementsInMemory(Integer maxElementsInMemory) {
		this.maxElementsInMemory = maxElementsInMemory;
	}

	public Long getTimeToIdleSeconds() {
		return timeToIdleSeconds;
	}

	public void setTimeToIdleSeconds(Long timeToIdleSeconds) {
		this.timeToIdleSeconds = timeToIdleSeconds;
	}

	public CacheType getCacheType() {
		return cacheType;
	}

	public void setCacheType(CacheType cacheType) {
		this.cacheType = cacheType;
	}

	public CacheMode getCacheMode() {
		return cacheMode;
	}

	public void setCacheMode(CacheMode cacheMode) {
		this.cacheMode = cacheMode;
	}

	public ArangoDBConnection getArangoDBConnection() {
		return arangoDBConnection;
	}

	public void setArangoDBConnection(ArangoDBConnection arangoDBConnection) {
		this.arangoDBConnection = arangoDBConnection;
	}

	public void setArangoDBConnection(ArrayList<String> host, ArrayList<Integer> portNumber, String dbName, String userName, String password, Integer timeout) {
		if (this.arangoDBConnection == null) {
			this.arangoDBConnection = new ArangoDBConnection();
		}

		this.arangoDBConnection.setHost(host);
		this.arangoDBConnection.setPortNumber(portNumber);
		this.arangoDBConnection.setDBName(dbName);
		this.arangoDBConnection.setUserName(userName);
		this.arangoDBConnection.setPassword(password);
		this.arangoDBConnection.setTimeout(timeout);
	}

	public String getInsertQuery() {
		return insertQuery;
	}

	public void setInsertQuery(String insertQuery) {
		this.insertQuery = insertQuery;
	}

	public ArrayList<Object> getRowInsertValues() {
		return rowInsertValues;
	}

	public void setRowInsertValues(ArrayList<Object> rowInsertValues) {
		this.rowInsertValues = rowInsertValues;
	}

	public ArrayList<Object> getCollectionInsertValues() {
		return collectionInsertValues;
	}

	public void setCollectionInsertValues(ArrayList<Object> collectionInsertValues) {
		this.collectionInsertValues = collectionInsertValues;
	}

	public boolean isWaitForSync() {
		return waitForSync;
	}

	public void setWaitForSync(boolean waitForSync) {
		this.waitForSync = waitForSync;
	}

}

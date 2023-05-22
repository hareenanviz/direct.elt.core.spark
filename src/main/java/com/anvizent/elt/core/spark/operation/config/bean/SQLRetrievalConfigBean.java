package com.anvizent.elt.core.spark.operation.config.bean;

import java.util.ArrayList;
import java.util.HashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.RetryMandatoryConfigBean;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.spark.constant.CacheMode;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SQLRetrievalConfigBean extends ConfigBean implements SimpleOperationConfigBean, RetryMandatoryConfigBean {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> selectColumns;
	private ArrayList<Integer> selectFieldPositions;
	private ArrayList<String> selectFieldAliases;
	private ArrayList<String> whereFields;
	private ArrayList<String> whereColumns;
	private ArrayList<String> insertValues;
	private ArrayList<String> insertValueByFields;
	private ArrayList<String> insertValuesByFieldFormats;
	private OnZeroFetchOperation onZeroFetch;
	private String tableName;
	private Integer maxElementsInMemory;
	private Long timeToIdleSeconds;
	private CacheType cacheType;
	private CacheMode cacheMode;
	private RDBMSConnection rdbmsConnection;
	private String customWhere;
	private boolean useAIValue;
	private Integer aiColumnIndex;
	private ArrayList<String> orderBy;
	private ArrayList<String> orderByTypes;
	private boolean keyFieldsCaseSensitive;
	private String selectQuery;
	private String insertQuery;
	private String bulkSelectQuery;
	private String customQueryAfterProcess;
	private ArrayList<String> selectAndWhereFields;
	private HashMap<String, Object> insertConstantValues;

	public ArrayList<String> getSelectColumns() {
		return selectColumns;
	}

	public void setSelectColumns(ArrayList<String> selectColumns) {
		this.selectColumns = selectColumns;
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

	public ArrayList<String> getInsertValues() {
		return insertValues;
	}

	public void setInsertValues(ArrayList<String> insertValues) {
		this.insertValues = insertValues;
	}

	public ArrayList<String> getInsertValueByFields() {
		return insertValueByFields;
	}

	public void setInsertValueByFields(ArrayList<String> insertValueByFields) {
		this.insertValueByFields = insertValueByFields;
	}

	public ArrayList<String> getInsertValuesByFieldFormats() {
		return insertValuesByFieldFormats;
	}

	public void setInsertValuesByFieldFormats(ArrayList<String> insertValuesByFieldFormats) {
		this.insertValuesByFieldFormats = insertValuesByFieldFormats;
	}

	public OnZeroFetchOperation getOnZeroFetch() {
		return onZeroFetch;
	}

	public void setOnZeroFetch(OnZeroFetchOperation onZeroFetch) {
		this.onZeroFetch = onZeroFetch;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
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

	public RDBMSConnection getRdbmsConnection() {
		return rdbmsConnection;
	}

	public void setRdbmsConnection(RDBMSConnection rdbmsConnection) {
		this.rdbmsConnection = rdbmsConnection;
	}

	public void setRdbmsConnection(String jdbcURL, String driver, String userName, String password) {
		if (this.rdbmsConnection == null) {
			this.rdbmsConnection = new RDBMSConnection();
		}
		this.rdbmsConnection.setDriver(driver);
		this.rdbmsConnection.setJdbcUrl(jdbcURL);
		this.rdbmsConnection.setUserName(userName);
		this.rdbmsConnection.setPassword(password);
	}

	public String getCustomWhere() {
		return customWhere;
	}

	public void setCustomWhere(String customWhere) {
		this.customWhere = customWhere;
	}

	public boolean isUseAIValue() {
		return useAIValue;
	}

	public void setUseAIValue(boolean useAIValue) {
		this.useAIValue = useAIValue;
	}

	public Integer getAiColumnIndex() {
		return aiColumnIndex;
	}

	public void setAiColumnIndex(Integer aiColumnIndex) {
		this.aiColumnIndex = aiColumnIndex;
	}

	public ArrayList<String> getOrderBy() {
		return orderBy;
	}

	public void setOrderBy(ArrayList<String> orderBy) {
		this.orderBy = orderBy;
	}

	public ArrayList<String> getOrderByTypes() {
		return orderByTypes;
	}

	public void setOrderByTypes(ArrayList<String> orderByTypes) {
		this.orderByTypes = orderByTypes;
	}

	public boolean isKeyFieldsCaseSensitive() {
		return keyFieldsCaseSensitive;
	}

	public void setKeyFieldsCaseSensitive(boolean keyFieldsCaseSensitive) {
		this.keyFieldsCaseSensitive = keyFieldsCaseSensitive;
	}

	public String getSelectQuery() {
		return selectQuery;
	}

	public void setSelectQuery(String selectQuery) {
		this.selectQuery = selectQuery;
	}

	public String getInsertQuery() {
		return insertQuery;
	}

	public void setInsertQuery(String insertQuery) {
		this.insertQuery = insertQuery;
	}

	public String getBulkSelectQuery() {
		return bulkSelectQuery;
	}

	public void setBulkSelectQuery(String bulkSelectQuery) {
		this.bulkSelectQuery = bulkSelectQuery;
	}

	public String getCustomQueryAfterProcess() {
		return customQueryAfterProcess;
	}

	public void setCustomQueryAfterProcess(String customQueryAfterProcess) {
		this.customQueryAfterProcess = customQueryAfterProcess;
	}

	public ArrayList<String> getSelectAndWhereFields() {
		return selectAndWhereFields;
	}

	public void setSelectAndWhereFields(ArrayList<String> selectAndWhereFields) {
		this.selectAndWhereFields = selectAndWhereFields;
	}

	public HashMap<String, Object> getInsertConstantValues() {
		return insertConstantValues;
	}

	public void setInsertConstantValues(HashMap<String, Object> insertConstantValues) {
		this.insertConstantValues = insertConstantValues;
	}
}

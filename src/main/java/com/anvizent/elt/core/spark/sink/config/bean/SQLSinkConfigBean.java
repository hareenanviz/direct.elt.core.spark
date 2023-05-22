package com.anvizent.elt.core.spark.sink.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.RetryMandatoryConfigBean;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.QueryInfo;
import com.anvizent.elt.core.spark.constant.BatchType;
import com.anvizent.elt.core.spark.constant.DBCheckMode;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.constant.DBWriteMode;
import com.anvizent.elt.core.spark.constant.SQLUpdateType;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkConfigBean extends ConfigBean implements SinkConfigBean, RetryMandatoryConfigBean, ErrorHandlerSink {

	private static final long serialVersionUID = 1L;

	private String tableName;
	private DBInsertMode dbInsertMode;
	private String deleteIndicatorField;
	private DBWriteMode dbWriteMode;
	private ArrayList<String> keyFields;
	private ArrayList<String> keyColumns;
	private ArrayList<String> fieldsDifferToColumns;
	private ArrayList<String> columnsDifferToFields;
	private ArrayList<String> metaDataFields;
	private boolean keyFieldsCaseSensitive;
	private String selectQuery;
	private String selectAllQuery;
	private String insertQuery;
	private QueryInfo upsertQueryInfo;
	private String updateQuery;
	private DBCheckMode dbCheckMode;
	private String deleteQuery;
	private String onConnectRunQuery;
	private String beforeComponentRunQuery;
	private String afterComponentSuccessRunQuery;
	private DBConstantsConfigBean constantsConfigBean = new DBConstantsConfigBean();
	private DBConstantsConfigBean insertConstantsConfigBean = new DBConstantsConfigBean();
	private DBConstantsConfigBean updateConstantsConfigBean = new DBConstantsConfigBean();
	private boolean alwaysUpdate;
	private SQLUpdateType updateUsing;
	private String checksumField;
	private RDBMSConnection rdbmsConnection;
	private RetryConfigBean initRetryConfigBean;
	private RetryConfigBean destroyRetryConfigBean;
	private BatchType batchType;
	private Long batchSize;
	private Integer prefetchBatchSize;
	private boolean removeOnceUsed = true;
	private ArrayList<String> rowFields;
	private ArrayList<String> rowKeysAndFields;
	private ArrayList<String> selectColumns;
	private ArrayList<String> selectFields;
	private ArrayList<String> insertFields;
	private ArrayList<String> updateFields;
	private Integer maxElementsInMemory;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public DBInsertMode getDBInsertMode() {
		return dbInsertMode;
	}

	public void setDBInsertMode(DBInsertMode dbInsertMode) {
		this.dbInsertMode = dbInsertMode;
	}

	public String getDeleteIndicatorField() {
		return deleteIndicatorField;
	}

	public void setDeleteIndicatorField(String deleteIndicatorField) {
		this.deleteIndicatorField = deleteIndicatorField;
	}

	public DBWriteMode getDBWriteMode() {
		return dbWriteMode;
	}

	public void setDBWriteMode(DBWriteMode dbWriteMode) {
		this.dbWriteMode = dbWriteMode;
	}

	public ArrayList<String> getKeyFields() {
		return keyFields;
	}

	public void setKeyFields(ArrayList<String> keyFields) {
		this.keyFields = keyFields;
	}

	public ArrayList<String> getKeyColumns() {
		return keyColumns;
	}

	public void setKeyColumns(ArrayList<String> keyColumns) {
		this.keyColumns = keyColumns;
	}

	public ArrayList<String> getFieldsDifferToColumns() {
		return fieldsDifferToColumns;
	}

	public void setFieldsDifferToColumns(ArrayList<String> fieldsDifferToColumns) {
		this.fieldsDifferToColumns = fieldsDifferToColumns;
	}

	public ArrayList<String> getColumnsDifferToFields() {
		return columnsDifferToFields;
	}

	public void setColumnsDifferToFields(ArrayList<String> columnsDifferToFields) {
		this.columnsDifferToFields = columnsDifferToFields;
	}

	public ArrayList<String> getMetaDataFields() {
		return metaDataFields;
	}

	public void setMetaDataFields(ArrayList<String> metaDataFields) {
		this.metaDataFields = metaDataFields;
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

	public String getSelectAllQuery() {
		return selectAllQuery;
	}

	public void setSelectAllQuery(String selectAllQuery) {
		this.selectAllQuery = selectAllQuery;
	}

	public String getInsertQuery() {
		return insertQuery;
	}

	public void setInsertQuery(String insertQuery) {
		this.insertQuery = insertQuery;
	}

	public QueryInfo getUpsertQueryInfo() {
		return upsertQueryInfo;
	}

	public void setUpsertQueryInfo(QueryInfo upsertQueryInfo) {
		this.upsertQueryInfo = upsertQueryInfo;
	}

	public String getUpdateQuery() {
		return updateQuery;
	}

	public void setUpdateQuery(String updateQuery) {
		this.updateQuery = updateQuery;
	}

	public DBCheckMode getDBCheckMode() {
		return dbCheckMode;
	}

	public void setDBCheckMode(DBCheckMode dbCheckMode) {
		this.dbCheckMode = dbCheckMode;
	}

	public String getDeleteQuery() {
		return deleteQuery;
	}

	public void setDeleteQuery(String deleteQuery) {
		this.deleteQuery = deleteQuery;
	}

	public String getOnConnectRunQuery() {
		return onConnectRunQuery;
	}

	public void setOnConnectRunQuery(String onConnectRunQuery) {
		this.onConnectRunQuery = onConnectRunQuery;
	}

	public String getBeforeComponentRunQuery() {
		return beforeComponentRunQuery;
	}

	public void setBeforeComponentRunQuery(String beforeComponentRunQuery) {
		this.beforeComponentRunQuery = beforeComponentRunQuery;
	}

	public String getAfterComponentSuccessRunQuery() {
		return afterComponentSuccessRunQuery;
	}

	public void setAfterComponentSuccessRunQuery(String afterComponentSuccessRunQuery) {
		this.afterComponentSuccessRunQuery = afterComponentSuccessRunQuery;
	}

	public DBConstantsConfigBean getConstantsConfigBean() {
		return constantsConfigBean;
	}

	public void setConstantsConfigBean(DBConstantsConfigBean constantsConfigBean) {
		this.constantsConfigBean = constantsConfigBean;
	}

	public DBConstantsConfigBean getInsertConstantsConfigBean() {
		return insertConstantsConfigBean;
	}

	public void setInsertConstantsConfigBean(DBConstantsConfigBean insertConstantsConfigBean) {
		this.insertConstantsConfigBean = insertConstantsConfigBean;
	}

	public DBConstantsConfigBean getUpdateConstantsConfigBean() {
		return updateConstantsConfigBean;
	}

	public void setUpdateConstantsConfigBean(DBConstantsConfigBean updateConstantsConfigBean) {
		this.updateConstantsConfigBean = updateConstantsConfigBean;
	}

	public boolean isAlwaysUpdate() {
		return alwaysUpdate;
	}

	public void setAlwaysUpdate(boolean alwaysUpdate) {
		this.alwaysUpdate = alwaysUpdate;
	}

	public SQLUpdateType getUpdateUsing() {
		return updateUsing;
	}

	public void setUpdateUsing(SQLUpdateType updateUsing) {
		this.updateUsing = updateUsing;
	}

	public String getChecksumField() {
		return checksumField;
	}

	public void setChecksumField(String checksumField) {
		this.checksumField = checksumField;
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

	public RetryConfigBean getInitRetryConfigBean() {
		return initRetryConfigBean;
	}

	public void setInitRetryConfigBean(RetryConfigBean initRetryConfigBean) {
		this.initRetryConfigBean = initRetryConfigBean;
	}

	public void setInitRetryConfigBean(Integer maxRetryCount, Long retryDelay) {
		this.initRetryConfigBean = new RetryConfigBean(maxRetryCount, retryDelay);
	}

	public RetryConfigBean getDestroyRetryConfigBean() {
		return destroyRetryConfigBean;
	}

	public void setDestroyRetryConfigBean(RetryConfigBean destroyRetryConfigBean) {
		this.destroyRetryConfigBean = destroyRetryConfigBean;
	}

	public void setDestroyRetryConfigBean(Integer maxRetryCount, Long retryDelay) {
		this.destroyRetryConfigBean = new RetryConfigBean(maxRetryCount, retryDelay);
	}

	public BatchType getBatchType() {
		return batchType;
	}

	public void setBatchType(BatchType batchType) {
		this.batchType = batchType;
	}

	public Long getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(Long batchSize) {
		this.batchSize = batchSize;
	}

	public Integer getPrefetchBatchSize() {
		return prefetchBatchSize;
	}

	public void setPrefetchBatchSize(Integer prefetchBatchSize) {
		this.prefetchBatchSize = prefetchBatchSize;
	}

	public boolean isRemoveOnceUsed() {
		return removeOnceUsed;
	}

	public void setRemoveOnceUsed(boolean removeOnceUsed) {
		this.removeOnceUsed = removeOnceUsed;
	}

	public ArrayList<String> getRowFields() {
		return rowFields;
	}

	public void setRowFields(ArrayList<String> rowFields) {
		this.rowFields = rowFields;
	}

	public ArrayList<String> getRowKeysAndFields() {
		return rowKeysAndFields;
	}

	public void setRowKeysAndFields(ArrayList<String> rowKeyAndFields) {
		this.rowKeysAndFields = rowKeyAndFields;
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

	public ArrayList<String> getInsertFields() {
		return insertFields;
	}

	public void setInsertFields(ArrayList<String> insertFields) {
		this.insertFields = insertFields;
	}

	public ArrayList<String> getUpdateFields() {
		return updateFields;
	}

	public void setUpdateFields(ArrayList<String> updateFields) {
		this.updateFields = updateFields;
	}

	public Integer getMaxElementsInMemory() {
		return maxElementsInMemory;
	}

	public void setMaxElementsInMemory(Integer maxElementsInMemory) {
		this.maxElementsInMemory = maxElementsInMemory;
	}
}

package com.anvizent.elt.core.spark.sink.config.bean;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.RetryMandatoryConfigBean;
import com.anvizent.elt.core.listener.common.connection.ArangoDBConnection;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.constant.BatchType;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.constant.DBWriteMode;

/**
 * @author Hareen Bejjanki
 *
 */
public class ArangoDBSinkConfigBean extends ConfigBean implements SinkConfigBean, RetryMandatoryConfigBean, ErrorHandlerSink {

	private static final long serialVersionUID = 1L;

	private String tableName;
	private DBInsertMode dbInsertMode;
	private DBWriteMode dbWriteMode;
	private ArrayList<String> keyFields;
	private ArrayList<String> keyColumns;
	private ArrayList<String> fieldsDifferToColumns;
	private ArrayList<String> columnsDifferToFields;
	private ArrayList<String> metaDataFields;
	private BatchType batchType;
	private Long batchSize;
	private boolean generateId;
	private boolean waitForSync;
	private RetryConfigBean initRetryConfigBean;
	private RetryConfigBean destroyRetryConfigBean;
	private NoSQLConstantsConfigBean constantsConfigBean = new NoSQLConstantsConfigBean();
	private NoSQLConstantsConfigBean insertConstantsConfigBean = new NoSQLConstantsConfigBean();
	private NoSQLConstantsConfigBean updateConstantsConfigBean = new NoSQLConstantsConfigBean();
	private ArangoDBConnection connection;
	private ArrayList<String> rowFields;
	private ArrayList<String> selectFields;
	private ArrayList<String> emptyArguments = new ArrayList<>();
	private ArrayList<Class<?>> emptyArgumentTypes = new ArrayList<>();
	private LinkedHashMap<String, Object> emptyRow = new LinkedHashMap<>();
	private String externalDataPrefix;

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

	public boolean isGenerateId() {
		return generateId;
	}

	public void setGenerateId(boolean generateId) {
		this.generateId = generateId;
	}

	public boolean isWaitForSync() {
		return waitForSync;
	}

	public void setWaitForSync(boolean waitForSync) {
		this.waitForSync = waitForSync;
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

	public NoSQLConstantsConfigBean getConstantsConfigBean() {
		return constantsConfigBean;
	}

	public void setConstantsConfigBean(NoSQLConstantsConfigBean constantsConfigBean) {
		this.constantsConfigBean = constantsConfigBean;
	}

	public NoSQLConstantsConfigBean getInsertConstantsConfigBean() {
		return insertConstantsConfigBean;
	}

	public void setInsertConstantsConfigBean(NoSQLConstantsConfigBean insertConstantsConfigBean) {
		this.insertConstantsConfigBean = insertConstantsConfigBean;
	}

	public NoSQLConstantsConfigBean getUpdateConstantsConfigBean() {
		return updateConstantsConfigBean;
	}

	public void setUpdateConstantsConfigBean(NoSQLConstantsConfigBean updateConstantsConfigBean) {
		this.updateConstantsConfigBean = updateConstantsConfigBean;
	}

	public ArangoDBConnection getConnection() {
		return connection;
	}

	public void setConnection(ArangoDBConnection connection) {
		this.connection = connection;
	}

	public void setConnection(ArrayList<String> host, ArrayList<Integer> portNumber, String dbName, String userName, String password, Integer timeout) {
		if (this.connection == null) {
			this.connection = new ArangoDBConnection();
		}

		this.connection.setHost(host);
		this.connection.setPortNumber(portNumber);
		this.connection.setDBName(dbName);
		this.connection.setUserName(userName);
		this.connection.setPassword(password);
		this.connection.setTimeout(timeout);
	}

	public ArrayList<String> getRowFields() {
		return rowFields;
	}

	public void setRowFields(ArrayList<String> rowFields) {
		this.rowFields = rowFields;
	}

	public ArrayList<String> getSelectFields() {
		return selectFields;
	}

	public void setSelectFields(ArrayList<String> selectFields) {
		this.selectFields = selectFields;
	}

	public ArrayList<String> getEmptyArguments() {
		return emptyArguments;
	}

	public ArrayList<Class<?>> getEmptyArgumentTypes() {
		return emptyArgumentTypes;
	}

	public LinkedHashMap<String, Object> getEmptyRow() {
		return emptyRow;
	}

	public String getExternalDataPrefix() {
		return externalDataPrefix;
	}

	public void setExternalDataPrefix(String externalDataPrefix) {
		this.externalDataPrefix = externalDataPrefix;
	}

}

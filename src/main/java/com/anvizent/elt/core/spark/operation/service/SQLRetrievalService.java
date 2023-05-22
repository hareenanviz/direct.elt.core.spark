package com.anvizent.elt.core.spark.operation.service;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.spark.TaskContext;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnectionByTaskId;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.operation.cache.SQLRetrievalCache;
import com.anvizent.elt.core.spark.operation.config.bean.SQLRetrievalConfigBean;

import net.sf.ehcache.CacheManager;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLRetrievalService implements Serializable {

	private static final long serialVersionUID = 1L;

	protected PreparedStatement bulkSelectPreparedStatement;
	protected PreparedStatement selectPreparedStatement;
	protected PreparedStatement insertPreparedStatement;
	protected SQLRetrievalCache sqlRetrievalCache;
	protected SQLRetrievalConfigBean sqlRetrievalConfigBean;
	protected LinkedHashMap<String, AnvizentDataType> structure;
	protected LinkedHashMap<String, AnvizentDataType> newStructure;

	public SQLRetrievalService(SQLRetrievalConfigBean sqlRetrievalConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure) throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		this.sqlRetrievalConfigBean = sqlRetrievalConfigBean;
		this.structure = structure;
		this.newStructure = newStructure;
		createConnection();
	}

	protected void createConnection() throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		Object[] connectionAndStatus = ApplicationConnectionBean.getInstance()
		        .get(new RDBMSConnectionByTaskId(sqlRetrievalConfigBean.getRdbmsConnection(), null, TaskContext.getPartitionId()), true);
		if ((boolean) connectionAndStatus[1]) {
			bulkSelectPreparedStatement = ((Connection) connectionAndStatus[0]).prepareStatement(sqlRetrievalConfigBean.getBulkSelectQuery());
			selectPreparedStatement = ((Connection) connectionAndStatus[0]).prepareStatement(sqlRetrievalConfigBean.getSelectQuery());
			insertPreparedStatement = ((Connection) connectionAndStatus[0]).prepareStatement(sqlRetrievalConfigBean.getInsertQuery(),
			        Statement.RETURN_GENERATED_KEYS);
		}
		if (sqlRetrievalConfigBean.getBulkSelectQuery() != null && !sqlRetrievalConfigBean.getBulkSelectQuery().isEmpty()
		        && (bulkSelectPreparedStatement == null || bulkSelectPreparedStatement.isClosed())) {
			bulkSelectPreparedStatement = ((Connection) ApplicationConnectionBean.getInstance()
			        .get(new RDBMSConnectionByTaskId(sqlRetrievalConfigBean.getRdbmsConnection(), null, TaskContext.getPartitionId()), true)[0])
			                .prepareStatement(sqlRetrievalConfigBean.getBulkSelectQuery());
		}

		if (selectPreparedStatement == null || selectPreparedStatement.isClosed()) {
			selectPreparedStatement = ((Connection) ApplicationConnectionBean.getInstance()
			        .get(new RDBMSConnectionByTaskId(sqlRetrievalConfigBean.getRdbmsConnection(), null, TaskContext.getPartitionId()), true)[0])
			                .prepareStatement(sqlRetrievalConfigBean.getSelectQuery());
		}

		if (insertPreparedStatement == null || insertPreparedStatement.isClosed()) {
			insertPreparedStatement = ((Connection) ApplicationConnectionBean.getInstance()
			        .get(new RDBMSConnectionByTaskId(sqlRetrievalConfigBean.getRdbmsConnection(), null, TaskContext.getPartitionId()), true)[0])
			                .prepareStatement(sqlRetrievalConfigBean.getInsertQuery(), Statement.RETURN_GENERATED_KEYS);
		}

		if (sqlRetrievalConfigBean.getCacheType().equals(CacheType.EHCACHE)) {
			createCache();
		}
	}

	private void createCache() {
		CacheManager cacheManager = CacheManager.getInstance();
		String cacheName = TaskContext.getPartitionId() + "_" + sqlRetrievalConfigBean.getName();

		if (sqlRetrievalCache == null && cacheManager.getCache(cacheName) == null) {
			sqlRetrievalCache = new SQLRetrievalCache(cacheName, sqlRetrievalConfigBean);
			cacheManager.addCache(sqlRetrievalCache);
		}
	}
}

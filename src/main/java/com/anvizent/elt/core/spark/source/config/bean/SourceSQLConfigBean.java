package com.anvizent.elt.core.spark.source.config.bean;

import java.util.LinkedHashMap;
import java.util.Map;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.constant.Constants.SQL;
import com.anvizent.elt.core.spark.source.resource.config.SQLResourceConfig;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SourceSQLConfigBean extends ConfigBean implements SourceConfigBean, SQLSource {
	private static final long serialVersionUID = 1L;

	private String tableName;
	private String rawQuery;
	private boolean isQuery;
	private RDBMSConnection connection;
	private Map<String, String> options = new LinkedHashMap<String, String>();
	private SQLResourceConfig resourceConfig;
	private String queryAlias;
	private Integer numberOfPartitions;
	private Long partitionsSize;
	private boolean queryContainsOffset;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
		this.rawQuery = tableName;
	}

	public void setTableName(boolean isQuery) {
		if (isQuery) {
			this.tableName = General.OPEN_PARENTHESIS + this.tableName + General.CLOSE_PARENTHESIS + " " + SQL.AS_TABLE;
		}
	}

	public String getRawQuery() {
		return rawQuery;
	}

	public void setRawQuery(String rawQuery) {
		this.rawQuery = rawQuery;
	}

	public boolean isQuery() {
		return isQuery;
	}

	public void setQuery(boolean isQuery) {
		this.isQuery = isQuery;
	}

	public RDBMSConnection getConnection() {
		return connection;
	}

	public void setConnection(RDBMSConnection connection) {
		this.connection = connection;
	}

	public void setConnection(String jdbcURL, String driver, String userName, String password) {
		if (this.connection == null) {
			this.connection = new RDBMSConnection();
		}
		this.connection.setDriver(driver);
		this.connection.setJdbcUrl(jdbcURL);
		this.connection.setUserName(userName);
		this.connection.setPassword(password);
	}

	public Map<String, String> getOptions() {
		return options;
	}

	public void setOptions(Map<String, String> options) {
		this.options = options;
	}

	public SQLResourceConfig getResourceConfig() {
		return resourceConfig;
	}

	public void setResourceConfig(SQLResourceConfig resourceConfig) {
		this.resourceConfig = resourceConfig;
	}

	public String getQueryAlias() {
		return queryAlias;
	}

	public void setQueryAlias(String queryAlias) {
		this.queryAlias = queryAlias;
	}

	public Integer getNumberOfPartitions() {
		return numberOfPartitions;
	}

	public void setNumberOfPartitions(Integer numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
	}

	public Long getPartitionsSize() {
		return partitionsSize;
	}

	public void setPartitionsSize(Long partitionsSize) {
		this.partitionsSize = partitionsSize;
	}

	public boolean isQueryContainsOffset() {
		return queryContainsOffset;
	}

	public void setQueryContainsOffset(boolean queryContainsOffset) {
		this.queryContainsOffset = queryContainsOffset;
	}

}

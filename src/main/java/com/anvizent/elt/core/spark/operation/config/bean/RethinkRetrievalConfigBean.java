package com.anvizent.elt.core.spark.operation.config.bean;

import java.time.ZoneOffset;
import java.util.ArrayList;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.connection.RethinkDBConnection;
import com.anvizent.elt.core.spark.constant.CacheMode;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;

/**
 * @author Hareen Bejjanki
 *
 */
public class RethinkRetrievalConfigBean extends ConfigBean {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> selectFields;
	private ArrayList<AnvizentDataType> selectFieldTypes;
	private ArrayList<String> selectFieldDateFormats;
	private ArrayList<Integer> selectFieldPositions;
	private ArrayList<String> selectFieldAliases;
	private ArrayList<String> eqFields;
	private ArrayList<String> eqColumns;
	private ArrayList<String> ltFields;
	private ArrayList<String> ltColumns;
	private ArrayList<String> gtFields;
	private ArrayList<String> gtColumns;
	private String tableName;
	private Integer maxElementsInMemory;
	private Long timeToIdleSeconds;
	private CacheType cacheType;
	private CacheMode cacheMode;
	private ArrayList<String> insertValues;
	private OnZeroFetchOperation onZeroFetch;
	private ArrayList<Integer> precisions;
	private ArrayList<Integer> scales;
	private RethinkDBConnection rethinkDBConnection;
	private ZoneOffset timeZoneOffset;
	private ArrayList<String> whereFields;
	private ArrayList<String> whereColumns;
	private ArrayList<Object> outputInsertValues;
	private ArrayList<Object> tableInsertValues;

	public ArrayList<String> getSelectFields() {
		return selectFields;
	}

	public void setSelectFields(ArrayList<String> selectFields) {
		this.selectFields = selectFields;
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

	public ArrayList<String> getEqFields() {
		return eqFields;
	}

	public void setEqFields(ArrayList<String> eqFields) {
		this.eqFields = eqFields;
	}

	public ArrayList<String> getEqColumns() {
		return eqColumns;
	}

	public void setEqColumns(ArrayList<String> eqColumns) {
		this.eqColumns = eqColumns;
	}

	public ArrayList<String> getLtFields() {
		return ltFields;
	}

	public void setLtFields(ArrayList<String> ltFields) {
		this.ltFields = ltFields;
	}

	public ArrayList<String> getLtColumns() {
		return ltColumns;
	}

	public void setLtColumns(ArrayList<String> ltColumns) {
		this.ltColumns = ltColumns;
	}

	public ArrayList<String> getGtFields() {
		return gtFields;
	}

	public void setGtFields(ArrayList<String> gtFields) {
		this.gtFields = gtFields;
	}

	public ArrayList<String> getGtColumns() {
		return gtColumns;
	}

	public void setGtColumns(ArrayList<String> gtColumns) {
		this.gtColumns = gtColumns;
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

	public ArrayList<Integer> getPrecisions() {
		return precisions;
	}

	public void setPrecisions(ArrayList<Integer> precisions) {
		this.precisions = precisions;
	}

	public ArrayList<Integer> getScales() {
		return scales;
	}

	public void setScales(ArrayList<Integer> scales) {
		this.scales = scales;
	}

	public RethinkDBConnection getRethinkDBConnection() {
		return rethinkDBConnection;
	}

	public void setRethinkDBConnection(RethinkDBConnection rethinkDBConnection) {
		this.rethinkDBConnection = rethinkDBConnection;
	}

	public void setRethinkDBConnection(ArrayList<String> host, ArrayList<Integer> portNumber, String dbName, String userName, String password, Long timeout) {
		if (this.rethinkDBConnection == null) {
			this.rethinkDBConnection = new RethinkDBConnection();
		}

		this.rethinkDBConnection.setHost(host);
		this.rethinkDBConnection.setPortNumber(portNumber);
		this.rethinkDBConnection.setDBName(dbName);
		this.rethinkDBConnection.setUserName(userName);
		this.rethinkDBConnection.setPassword(password);
		this.rethinkDBConnection.setTimeout(timeout);
	}

	public void setStructure(ArrayList<String> lookUpFieldNames, ArrayList<Class<?>> selectFieldDataTypes) throws UnsupportedException {
		if (lookUpFieldNames != null && selectFieldDataTypes != null) {
			this.selectFieldTypes = new ArrayList<>();

			for (int i = 0; i < selectFieldDataTypes.size(); i++) {
				Class<?> type = selectFieldDataTypes.get(i);
				int precision = precisions == null ? General.DECIMAL_PRECISION : (precisions.get(i) == null ? General.DECIMAL_PRECISION : precisions.get(i));
				int scale = scales == null ? General.DECIMAL_SCALE : (scales.get(i) == null ? General.DECIMAL_SCALE : scales.get(i));
				this.selectFieldTypes.add(new AnvizentDataType(type, precision, scale));
			}
		} else {
			this.selectFieldTypes = null;
		}
	}

	public ZoneOffset getTimeZoneOffset() {
		return timeZoneOffset;
	}

	public void setTimeZoneOffset(ZoneOffset timeZoneOffset) {
		this.timeZoneOffset = timeZoneOffset;
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

	public ArrayList<Object> getOutputInsertValues() {
		return outputInsertValues;
	}

	public void setOutputInsertValues(ArrayList<Object> outputInsertValues) {
		this.outputInsertValues = outputInsertValues;
	}

	public ArrayList<Object> getTableInsertValues() {
		return tableInsertValues;
	}

	public void setTableInsertValues(ArrayList<Object> tableInsertValues) {
		this.tableInsertValues = tableInsertValues;
	}
}

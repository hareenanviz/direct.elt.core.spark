package com.anvizent.elt.core.spark.source.config.bean;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.util.ArrayList;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.RetryMandatoryConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.connection.RethinkDBConnection;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SourceRethinkDBConfigBean extends ConfigBean implements SourceConfigBean, RetryMandatoryConfigBean, CoreSource {

	private static final long serialVersionUID = 1L;

	private String tableName;
	private ArrayList<String> selectFields;
	private ArrayList<Class<?>> selectFieldTypes;
	private StructType structType;
	private RethinkDBConnection connection;
	private PartitionConfigBean partitionConfigBean;
	private String partitionType;
	private BigDecimal limit;
	private Long partitionSize;
	private ZoneOffset timeZoneOffset;

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

	public ArrayList<Class<?>> getSelectFieldTypes() {
		return selectFieldTypes;
	}

	public void setSelectFieldTypes(ArrayList<Class<?>> selectFieldTypes) {
		this.selectFieldTypes = selectFieldTypes;
	}

	@Override
	public StructType getStructType() {
		return structType;
	}

	public void setStructType(StructType structType) {
		this.structType = structType;
	}

	public void setStructType(ArrayList<String> selectFields, ArrayList<Class<?>> selectFieldTypes) throws ImproperValidationException, UnsupportedException {
		if (selectFields != null && !selectFields.isEmpty() && selectFieldTypes != null && !selectFieldTypes.isEmpty()
		        && selectFields.size() == selectFieldTypes.size()) {
			StructField[] structFields = new StructField[selectFields.size()];

			for (int i = 0; i < selectFields.size(); i++) {
				structFields[i] = DataTypes.createStructField(selectFields.get(i), new AnvizentDataType(selectFieldTypes.get(i)).getSparkType(), true);
			}

			this.structType = DataTypes.createStructType(structFields);
		}
	}

	public RethinkDBConnection getConnection() {
		return connection;
	}

	public void setConnection(RethinkDBConnection connection) {
		this.connection = connection;
	}

	public void setConnection(ArrayList<String> host) {
		if (this.connection == null) {
			this.connection = new RethinkDBConnection();
		}
		this.connection.setHost(host);
	}

	public void setConnection(ArrayList<String> host, ArrayList<Integer> portNumber, String dbName, String userName, String password, Long timeout) {
		if (this.connection == null) {
			this.connection = new RethinkDBConnection();
		}
		this.connection.setHost(host);
		this.connection.setPortNumber(portNumber);
		this.connection.setDBName(dbName);
		this.connection.setUserName(userName);
		this.connection.setPassword(password);
		this.connection.setTimeout(timeout);
	}

	public PartitionConfigBean getPartitionConfigBean() {
		return partitionConfigBean;
	}

	public void setPartitionConfigBean(PartitionConfigBean partitionConfigBean) {
		this.partitionConfigBean = partitionConfigBean;
	}

	public String getPartitionType() {
		return partitionType;
	}

	public void setPartitionType(String partitionType) {
		this.partitionType = partitionType;
	}

	public BigDecimal getLimit() {
		return limit;
	}

	public void setLimit(BigDecimal limit) {
		this.limit = limit;
	}

	public Long getPartitionSize() {
		return partitionSize;
	}

	public void setPartitionSize(Long partitionSize) {
		this.partitionSize = partitionSize;
	}

	public ZoneOffset getTimeZoneOffset() {
		return timeZoneOffset;
	}

	public void setTimeZoneOffset(ZoneOffset timeZoneOffset) {
		this.timeZoneOffset = timeZoneOffset;
	}

}
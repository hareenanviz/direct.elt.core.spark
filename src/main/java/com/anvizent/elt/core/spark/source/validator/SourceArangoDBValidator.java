package com.anvizent.elt.core.spark.source.validator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.connection.ArangoDBConnection;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.ArangoDBDefault;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Source.SourceArangoDB;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.source.config.bean.PartitionConfigBean;
import com.anvizent.elt.core.spark.source.config.bean.SourceArangoDBConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class SourceArangoDBValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public SourceArangoDBValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {
		validateMandatoryFields(configs, SQLNoSQL.HOST, SQLNoSQL.PORT_NUMBER, SQLNoSQL.TABLE);

		SourceArangoDBConfigBean configBean = new SourceArangoDBConfigBean();
		validateAndSetConfigBean(configBean, configs);

		return configBean;
	}

	private void validateAndSetConfigBean(SourceArangoDBConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, UnsupportedException {
		setConnection(configBean, configs);
		setConfigBean(configBean, configs);
		validateConfigBean(configBean);
	}

	private void validateConfigBean(SourceArangoDBConfigBean configBean) {
		validateSelectFields(configBean);
		validatePartitionDetails(configBean, configBean.getPartitionConfigBean());
	}

	private void validatePartitionDetails(SourceArangoDBConfigBean configBean, PartitionConfigBean partitionConfigBean) {
		if (partitionConfigBean.getPartitionColumns() != null && !partitionConfigBean.getPartitionColumns().isEmpty()) {

			if (partitionConfigBean.getPartitionColumns().size() > 1) {
				if (partitionConfigBean.getLowerBound() != null) {
					exception.add(Message.KEY_IS_ALLOWED_ONLY_FOR_SINGLE_OTHER_KEY, SourceArangoDB.LOWER_BOUND, SourceArangoDB.PARTITION_COLUMNS);
				}

				if (partitionConfigBean.getUpperBound() != null) {
					exception.add(Message.KEY_IS_ALLOWED_ONLY_FOR_SINGLE_OTHER_KEY, SourceArangoDB.UPPER_BOUND, SourceArangoDB.PARTITION_COLUMNS);
				}
			} else {
				if (partitionConfigBean.getLowerBound() == null && partitionConfigBean.getUpperBound() != null) {
					exception.add(Message.INVALID_WHEN_OTHER_NOT_PRECENT, SourceArangoDB.UPPER_BOUND, SourceArangoDB.LOWER_BOUND);
				}

				if (partitionConfigBean.getLowerBound() != null && partitionConfigBean.getUpperBound() == null) {
					exception.add(Message.INVALID_WHEN_OTHER_NOT_PRECENT, SourceArangoDB.LOWER_BOUND, SourceArangoDB.UPPER_BOUND);
				}

				if (partitionConfigBean.getLowerBound() != null && partitionConfigBean.getUpperBound() != null) {
					if (partitionConfigBean.getLowerBound().compareTo(partitionConfigBean.getUpperBound()) == 0) {
						exception.add(Message.KEY_CANNOT_BE_EQUAL_TO_OTHER_KEY, SourceArangoDB.LOWER_BOUND, SourceArangoDB.UPPER_BOUND);
					}

					if (partitionConfigBean.getLowerBound().compareTo(partitionConfigBean.getUpperBound()) > 0) {
						exception.add(Message.KEY_CANNOT_BE_GREATER_THAN_OTHER_KEY, SourceArangoDB.LOWER_BOUND, SourceArangoDB.UPPER_BOUND);
					}
				}
			}

			if (configBean.getPartitionSize() != null) {
				exception.add(Message.INVALID_WHEN_OTHER_PRECENT, SourceArangoDB.PARTITION_SIZE, SourceArangoDB.PARTITION_COLUMNS);
			}

			if (partitionConfigBean.getNumberOfPartitions() == null) {
				exception.add(Message.SINGLE_KEY_MANDATORY, SourceArangoDB.NUMBER_OF_PARTITIONS);
			} else if (partitionConfigBean.getNumberOfPartitions() <= 0) {
				exception.add(Message.KEY_IS_INVALID, SourceArangoDB.NUMBER_OF_PARTITIONS);
			}
		} else {
			if (partitionConfigBean.getLowerBound() != null) {
				exception.add(Message.INVALID_WHEN_OTHER_NOT_PRECENT, SourceArangoDB.LOWER_BOUND, SourceArangoDB.PARTITION_COLUMNS);
			}

			if (partitionConfigBean.getUpperBound() != null) {
				exception.add(Message.INVALID_WHEN_OTHER_NOT_PRECENT, SourceArangoDB.UPPER_BOUND, SourceArangoDB.PARTITION_COLUMNS);
			}

			if (partitionConfigBean.getNumberOfPartitions() != null) {
				exception.add(Message.INVALID_WHEN_OTHER_NOT_PRECENT, SourceArangoDB.NUMBER_OF_PARTITIONS, SourceArangoDB.PARTITION_COLUMNS);
			}
		}

		if (configBean.getPartitionSize() != null && configBean.getPartitionSize() <= 0) {
			exception.add(Message.KEY_IS_INVALID, SourceArangoDB.PARTITION_SIZE);
		}

		setPartitionType(configBean, partitionConfigBean);
	}

	private void setPartitionType(SourceArangoDBConfigBean configBean, PartitionConfigBean partitionConfigBean) {
		if ((partitionConfigBean.getPartitionColumns() == null || partitionConfigBean.getPartitionColumns().isEmpty())
		        && configBean.getPartitionSize() == null) {
			configBean.setPartitionType(General.NO_PARTITION);
		} else if ((partitionConfigBean.getPartitionColumns() == null || partitionConfigBean.getPartitionColumns().isEmpty())
		        && configBean.getPartitionSize() != null) {
			configBean.setPartitionType(General.BATCH_PARTITION_TYPE);
		} else if ((partitionConfigBean.getPartitionColumns() != null && !partitionConfigBean.getPartitionColumns().isEmpty())
		        && (partitionConfigBean.getLowerBound() == null && partitionConfigBean.getUpperBound() == null)) {
			configBean.setPartitionType(General.DISTINCT_FIELDS_PARTITION_TYPE);
		} else {
			configBean.setPartitionType(General.RANGE_PARTITION_TYPE);
		}
	}

	private void validateSelectFields(SourceArangoDBConfigBean configBean) {
		if (configBean.getSelectFields() == null || configBean.getSelectFields().isEmpty()) {
			exception.add(Message.SINGLE_KEY_MANDATORY, SourceArangoDB.SELECT_FIELDS);
		}

		if (configBean.getSelectFieldTypes() == null || configBean.getSelectFieldTypes().isEmpty()) {
			exception.add(Message.SINGLE_KEY_MANDATORY, SourceArangoDB.SELECT_FIELD_TYPES);
		}

		if (configBean.getSelectFields() != null && configBean.getSelectFieldTypes() != null
		        && configBean.getSelectFields().size() != configBean.getSelectFieldTypes().size()) {
			exception.add(Message.SIZE_SHOULD_MATCH, SourceArangoDB.SELECT_FIELDS, SourceArangoDB.SELECT_FIELD_TYPES);
		}

		if (configBean.getLimit() != null && configBean.getLimit().compareTo(BigDecimal.valueOf(0)) <= 0) {
			exception.add(Message.KEY_IS_INVALID, SourceArangoDB.LIMIT);
		}
	}

	private void setConfigBean(SourceArangoDBConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, UnsupportedException {
		String tableName = ConfigUtil.getString(configs, SQLNoSQL.TABLE);
		ArrayList<String> selectFields = ConfigUtil.getArrayList(configs, SourceArangoDB.SELECT_FIELDS, exception);
		ArrayList<Class<?>> selectFieldTypes = ConfigUtil.getArrayListOfClass(configs, SourceArangoDB.SELECT_FIELD_TYPES, exception);
		String whereClause = ConfigUtil.getString(configs, SourceArangoDB.WHERE_CLAUSE);

		configBean.setTableName(tableName);
		configBean.setSelectFields(selectFields);
		configBean.setSelectFieldTypes(selectFieldTypes);
		configBean.setWhereClause(whereClause);
		configBean.setStructType(selectFields, selectFieldTypes);
		configBean.setLimit(ConfigUtil.getBigDecimal(configs, SourceArangoDB.LIMIT, exception));
		configBean.setPartitionSize(ConfigUtil.getLong(configs, SourceArangoDB.PARTITION_SIZE, exception));

		setPartitionDetails(configBean, configs);
	}

	private void setPartitionDetails(SourceArangoDBConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		PartitionConfigBean partitionConfigBean = new PartitionConfigBean();

		partitionConfigBean.setPartitionColumns(ConfigUtil.getArrayList(configs, SourceArangoDB.PARTITION_COLUMNS, exception));
		partitionConfigBean.setLowerBound(ConfigUtil.getBigDecimal(configs, SourceArangoDB.LOWER_BOUND, exception));
		partitionConfigBean.setUpperBound(ConfigUtil.getBigDecimal(configs, SourceArangoDB.UPPER_BOUND, exception));
		partitionConfigBean.setNumberOfPartitions(ConfigUtil.getInteger(configs, SourceArangoDB.NUMBER_OF_PARTITIONS, exception));

		configBean.setPartitionConfigBean(partitionConfigBean);
	}

	private void setConnection(SourceArangoDBConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		ArrayList<String> host = ConfigUtil.getArrayList(configs, SQLNoSQL.HOST, exception);
		ArrayList<Integer> portNumber = ConfigUtil.getArrayListOfIntegers(configs, SQLNoSQL.PORT_NUMBER, exception);
		String dbName = ConfigUtil.getString(configs, SQLNoSQL.DB_NAME, ArangoDBDefault.DB_NAME);
		String userName = ConfigUtil.getString(configs, SQLNoSQL.USER_NAME);
		String password = ConfigUtil.getString(configs, SQLNoSQL.PASSWORD);
		Integer timeout = ConfigUtil.getInteger(configs, SQLNoSQL.TIMEOUT, exception, ArangoDBDefault.TIMEOUT);

		configBean.setConnection(host, portNumber, dbName, userName, password, timeout);

		validateConnection(configBean.getConnection());
	}

	private void validateConnection(ArangoDBConnection arangoDBConnection) {
		if (arangoDBConnection.getHost() != null && !arangoDBConnection.getHost().isEmpty() && arangoDBConnection.getPortNumber() != null
		        && !arangoDBConnection.getPortNumber().isEmpty() && arangoDBConnection.getHost().size() != arangoDBConnection.getPortNumber().size()) {
			exception.add(Message.SIZE_SHOULD_MATCH, SQLNoSQL.HOST, SQLNoSQL.PORT_NUMBER);
		}
	}

}

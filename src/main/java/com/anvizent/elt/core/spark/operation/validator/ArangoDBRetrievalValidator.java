package com.anvizent.elt.core.spark.operation.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.commons.lang3.StringUtils;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.CacheMode;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.ArangoDBDefault;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.ArangoDBLookUp;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBFetcherConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBLookUpConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBRetrievalConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class ArangoDBRetrievalValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public ArangoDBRetrievalValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {
		validateMandatoryFields(configs, SQLNoSQL.HOST, SQLNoSQL.PORT_NUMBER, SQLNoSQL.TABLE);

		ArangoDBRetrievalConfigBean configBean = null;

		if (this instanceof ArangoDBLookUpValidator) {
			configBean = new ArangoDBLookUpConfigBean();
		} else {
			configBean = new ArangoDBFetcherConfigBean();
		}

		setConfigBean(configBean, configs);
		setArangoDBRetrievalConfigBean(configBean, configs);

		validateConfigBean(configBean, configs);
		validateArangoDBRetrievalConfigBean(configBean, configs);

		return configBean;
	}

	private void validateConfigBean(ArangoDBRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {

		validateMandatoryFields(configs, Operation.General.SELECT_COLUMNS, ArangoDBLookUp.SELECT_FIELD_TYPES);

		if (configBean.getSelectFields() != null && !configBean.getSelectFields().isEmpty()) {
			if (configBean.getSelectFieldPositions() != null && configBean.getSelectFields().size() != configBean.getSelectFieldPositions().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.SELECT_COLUMNS, Operation.General.SELECT_FIELD_POSITIONS);
			}

			if (configBean.getSelectFieldAliases() != null && configBean.getSelectFields().size() != configBean.getSelectFieldAliases().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.SELECT_COLUMNS, Operation.General.SELECT_COLUMNS_AS_FIELDS);
			}

			if (configBean.getSelectFieldTypes() != null && configBean.getSelectFields().size() != configBean.getSelectFieldTypes().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.SELECT_COLUMNS, Operation.ArangoDBLookUp.SELECT_FIELD_TYPES);
			}

			if (configBean.getSelectFieldDateFormats() != null && configBean.getSelectFields().size() != configBean.getSelectFieldDateFormats().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.SELECT_COLUMNS,
				        Operation.ArangoDBLookUp.SELECT_FIELD_DATE_FORMATS);
			}

			if (configBean.getCustomWhere() == null || configBean.getCustomWhere().isEmpty()) {
				if (configBean.getOnZeroFetch() == null) {
					exception.add(ValidationConstant.Message.KEY_IS_INVALID, Operation.General.ON_ZERO_FETCH);
				} else if (configBean.getOnZeroFetch().equals(OnZeroFetchOperation.INSERT)) {
					if (configBean.getInsertValues() == null || configBean.getInsertValues().isEmpty()) {
						exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, Operation.General.INSERT_VALUES);
					} else if (configBean.getSelectFields().size() != configBean.getInsertValues().size()) {
						exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.SELECT_COLUMNS, Operation.General.INSERT_VALUES);
					}
				} else if ((configBean.getOnZeroFetch().equals(OnZeroFetchOperation.FAIL) || configBean.getOnZeroFetch().equals(OnZeroFetchOperation.IGNORE))
				        && configBean.getInsertValues() != null && !configBean.getInsertValues().isEmpty()) {
					exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_PRECENT, Operation.General.ON_ZERO_FETCH,
					        Operation.General.INSERT_VALUES + ":" + configBean.getOnZeroFetch());
				}
			} else {
				if (configBean.getOnZeroFetch() == null) {
					exception.add(ValidationConstant.Message.KEY_IS_INVALID, Operation.General.ON_ZERO_FETCH);
				} else if (configBean.getOnZeroFetch().equals(OnZeroFetchOperation.INSERT)) {
					exception.add(ValidationConstant.Message.INVALID_VALUE_FOR, Operation.General.ON_ZERO_FETCH, Operation.General.CUSTOM_WHERE);
				} else if ((configBean.getOnZeroFetch().equals(OnZeroFetchOperation.FAIL) || configBean.getOnZeroFetch().equals(OnZeroFetchOperation.IGNORE))
				        && configBean.getInsertValues() != null && !configBean.getInsertValues().isEmpty()) {
					exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_PRECENT, Operation.General.ON_ZERO_FETCH,
					        Operation.General.INSERT_VALUES + ":" + configBean.getOnZeroFetch());
				}
			}
		}

		validateCustomWhereAndWhereFields(configBean, configs);
		validateOrderByFields(configBean);
		validateCacheSettings(configBean);
	}

	private void validateOrderByFields(ArangoDBRetrievalConfigBean configBean) {
		if (configBean.getOrderByType() != null && !configBean.getOrderByType().isEmpty()
		        && (configBean.getOrderByFields() == null || configBean.getOrderByFields().isEmpty())) {
			exception.add(ValidationConstant.Message.IS_MANDATORY_WHEN_PRESENT, Operation.ArangoDBLookUp.ORDER_BY_FIELDS, ArangoDBLookUp.ORDER_BY_TYPE);
		} else if ((configBean.getOrderByType() == null || configBean.getOrderByType().isEmpty()) && configBean.getOrderByFields() != null
		        && !configBean.getOrderByFields().isEmpty()) {
			exception.add(ValidationConstant.Message.IS_MANDATORY_WHEN_PRESENT, ArangoDBLookUp.ORDER_BY_TYPE, Operation.ArangoDBLookUp.ORDER_BY_FIELDS);
		} else if (configBean.getOrderByType() != null && !(configBean.getOrderByType().equals("ASC") || configBean.getOrderByType().equals("DESC"))) {
			exception.add(ValidationConstant.Message.KEY_IS_INVALID, ArangoDBLookUp.ORDER_BY_TYPE);
		}
	}

	private void validateCustomWhereAndWhereFields(ArangoDBRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) {
		if (ConfigUtil.isAllEmpty(configs, Operation.General.WHERE_FIELDS, Operation.General.CUSTOM_WHERE)) {
			exception.add(Message.EITHER_OF_THE_KEY_IS_MANDATORY, Operation.General.WHERE_FIELDS, Operation.General.CUSTOM_WHERE);
		} else if (ConfigUtil.isAllNotEmpty(configs, Operation.General.WHERE_FIELDS, Operation.General.CUSTOM_WHERE)) {
			exception.add(Message.EITHER_OF_THE_KEY_IS_MANDATORY, Operation.General.WHERE_FIELDS, Operation.General.CUSTOM_WHERE);
		} else if (configBean.getWhereFields() != null && !configBean.getWhereFields().isEmpty()
		        && (configBean.getCustomWhere() == null || configBean.getCustomWhere().isEmpty())) {
			if (configBean.getWhereColumns() != null && configBean.getWhereColumns().size() != configBean.getWhereFields().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.WHERE_FIELDS, Operation.General.WHERE_COLUMNS);
			}
		}
	}

	private void validateCacheSettings(ArangoDBRetrievalConfigBean configBean) {
		if (configBean.getCacheType() == null) {
			exception.add(ValidationConstant.Message.KEY_IS_INVALID, Operation.General.CACHE_TYPE);
		} else if (configBean.getCacheType().equals(CacheType.NONE) || configBean.getCacheType().equals(CacheType.MEMCACHE)) {
			if (configBean.getCacheMode() != null) {
				exception.add("'" + configBean.getCacheMode() + "'" + ExceptionMessage.NOT_ALLOWED_FOR + "'" + configBean.getCacheType() + "' "
				        + ExceptionMessage.CACHE_TYPE);
			}
		} else if (configBean.getCacheType().equals(CacheType.ELASTIC_CACHE)) {
			exception.add(ValidationConstant.Message.UNSUPPORTED, configBean.getCacheType().name(), Operation.General.CACHE_TYPE);
			if (configBean.getCacheMode() == null) {
				exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, Operation.General.CACHE_MODE);
			} else if (!configBean.getCacheMode().equals(CacheMode.CLUSTER)) {
				exception.add(ExceptionMessage.ELASTIC_CACHE_ALLOWED_FOR_CLUSTER_CACHE_TYPE);
			}
		} else if (configBean.getCacheType().equals(CacheType.EHCACHE)) {
			if (configBean.getCacheMode() == null) {
				exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, Operation.General.CACHE_MODE);
			} else if (!configBean.getCacheMode().equals(CacheMode.LOCAL)) {
				exception.add(ExceptionMessage.EHCACHE_ALLOWED_ONLY_FOR_LOCAL_CACHE_TYPE);
			}

			if (configBean.getTimeToIdleSeconds() == null) {
				exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, Operation.General.CACHE_TIME_TO_IDLE);
			}

			if (configBean.getMaxElementsInMemory() == null) {
				exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, General.CACHE_MAX_ELEMENTS_IN_MEMORY);
			}
		}
	}

	private void setConfigBean(ArangoDBRetrievalConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, InvalidConfigException, UnsupportedException {

		configBean.setTableName(ConfigUtil.getString(configs, SQLNoSQL.TABLE));

		ArrayList<String> selectFields = ConfigUtil.getArrayList(configs, Operation.General.SELECT_COLUMNS, exception);
		ArrayList<Class<?>> selectFieldDataTypes = ConfigUtil.getArrayListOfClass(configs, ArangoDBLookUp.SELECT_FIELD_TYPES, exception);
		ArrayList<String> selectFieldAliases = ConfigUtil.getArrayList(configs, Operation.General.SELECT_COLUMNS_AS_FIELDS, exception);
		ArrayList<Integer> positions = ConfigUtil.getArrayListOfIntegers(configs, Operation.General.SELECT_FIELD_POSITIONS, exception);

		configBean.setSelectFields(selectFields);
		configBean.setStructure(selectFieldAliases == null || selectFieldAliases.isEmpty() ? selectFields : selectFieldAliases, selectFieldDataTypes);
		configBean.setSelectFieldPositions(positions != null && !positions.isEmpty() ? positions : null);
		configBean.setSelectFieldAliases(selectFieldAliases != null && !selectFieldAliases.isEmpty() ? selectFieldAliases : null);
		configBean.setSelectFieldDateFormats(ConfigUtil.getArrayList(configs, ArangoDBLookUp.SELECT_FIELD_DATE_FORMATS, exception));

		configBean.setWhereFields(ConfigUtil.getArrayList(configs, Operation.General.WHERE_FIELDS, exception));
		configBean.setWhereColumns(ConfigUtil.getArrayList(configs, Operation.General.WHERE_COLUMNS, exception));
		configBean.setCustomWhere(ConfigUtil.getString(configs, Operation.General.CUSTOM_WHERE));

		configBean.setInsertValues(ConfigUtil.getArrayList(configs, Operation.General.INSERT_VALUES, exception));
		configBean.setOnZeroFetch(OnZeroFetchOperation.getInstance(ConfigUtil.getString(configs, Operation.General.ON_ZERO_FETCH)));

		configBean.setOrderByFields(ConfigUtil.getArrayList(configs, Operation.ArangoDBLookUp.ORDER_BY_FIELDS, exception));
		configBean.setOrderByType(ConfigUtil.getString(configs, ArangoDBLookUp.ORDER_BY_TYPE));

		configBean.setWaitForSync(ConfigUtil.getBoolean(configs, ArangoDBLookUp.WAIT_FOR_SYNC, exception, true));

		setConnection(configBean, configs);
		setCacheSettings(configBean, configs);
	}

	private void setConnection(ArangoDBRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		ArrayList<String> host = ConfigUtil.getArrayList(configs, SQLNoSQL.HOST, exception);
		ArrayList<Integer> portNumber = ConfigUtil.getArrayListOfIntegers(configs, SQLNoSQL.PORT_NUMBER, exception);
		String dbName = ConfigUtil.getString(configs, SQLNoSQL.DB_NAME, ArangoDBDefault.DB_NAME);
		String userName = ConfigUtil.getString(configs, SQLNoSQL.USER_NAME);
		String password = ConfigUtil.getString(configs, SQLNoSQL.PASSWORD);
		Integer timeout = ConfigUtil.getInteger(configs, SQLNoSQL.TIMEOUT, exception, ArangoDBDefault.TIMEOUT);

		configBean.setArangoDBConnection(host, portNumber, dbName, userName, password, timeout);
	}

	private void setCacheSettings(ArangoDBRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) {
		configBean.setCacheType(CacheType.getInstance(ConfigUtil.getString(configs, Operation.General.CACHE_TYPE)));
		configBean.setCacheMode(CacheMode.getInstance(ConfigUtil.getString(configs, Operation.General.CACHE_MODE)));
		configBean.setTimeToIdleSeconds(StringUtils.isEmpty(configs.get(Operation.General.CACHE_TIME_TO_IDLE)) ? null
		        : Long.parseLong(configs.get(Operation.General.CACHE_TIME_TO_IDLE)));
		configBean.setMaxElementsInMemory(StringUtils.isEmpty(configs.get(General.CACHE_MAX_ELEMENTS_IN_MEMORY)) ? null
		        : Integer.parseInt(configs.get(General.CACHE_MAX_ELEMENTS_IN_MEMORY)));
	}

	protected abstract void setArangoDBRetrievalConfigBean(ArangoDBRetrievalConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws InvalidConfigException;

	protected abstract void validateArangoDBRetrievalConfigBean(ArangoDBRetrievalConfigBean configBean, LinkedHashMap<String, String> configs);
}

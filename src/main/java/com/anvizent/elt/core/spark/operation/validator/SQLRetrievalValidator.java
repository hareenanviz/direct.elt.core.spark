package com.anvizent.elt.core.spark.operation.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.commons.lang3.StringUtils;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.CacheMode;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.SQLLookUp;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.SQLFetcherConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.SQLLookUpConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.SQLRetrievalConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class SQLRetrievalValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public SQLRetrievalValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException {
		validateMandatoryFields(configs, SQLNoSQL.JDBC_URL, SQLNoSQL.DRIVER, SQLNoSQL.USER_NAME, SQLNoSQL.PASSWORD, SQLNoSQL.TABLE);

		SQLRetrievalConfigBean configBean = null;

		if (this instanceof SQLLookUpValidator) {
			configBean = new SQLLookUpConfigBean();
		} else {
			configBean = new SQLFetcherConfigBean();
		}

		setConfigBean(configBean, configs);
		setSQLRetrievalConfigBean(configBean, configs);

		validateConfigBean(configBean, configs);
		validateSQLRetrievalConfigBean(configBean, configs);

		return configBean;
	}

	private void validateConfigBean(SQLRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {

		validateMandatoryFields(configs, Operation.General.SELECT_COLUMNS);

		if (configBean.getSelectColumns() != null) {
			if (configBean.getSelectFieldPositions() != null && configBean.getSelectColumns().size() != configBean.getSelectFieldPositions().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.SELECT_COLUMNS, Operation.General.SELECT_FIELD_POSITIONS);
			}

			if (configBean.getSelectFieldAliases() != null && configBean.getSelectColumns().size() != configBean.getSelectFieldAliases().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.SELECT_COLUMNS, Operation.General.SELECT_COLUMNS_AS_FIELDS);
			}

			if (configBean.getCustomWhere() == null) {
				if (configBean.getOnZeroFetch().equals(OnZeroFetchOperation.INSERT)) {
					validateMergingConfig(configBean.getInsertValues(), configBean.getInsertValueByFields(), Operation.General.INSERT_VALUES,
					        Operation.General.INSERT_VALUE_BY_FIELDS, configBean.getSelectColumns().size(), Operation.General.SELECT_COLUMNS);
					validateParellelConfig(configBean.getInsertValueByFields(), configBean.getInsertValuesByFieldFormats(),
					        Operation.General.INSERT_VALUE_BY_FIELDS, Operation.General.INSERT_VALUE_BY_FIELD_FORMATS);
				}
			} else {
				if (configBean.getOnZeroFetch().equals(OnZeroFetchOperation.INSERT)) {
					exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_IS, Operation.General.CUSTOM_WHERE, Operation.General.ON_ZERO_FETCH,
					        OnZeroFetchOperation.INSERT);
				}
			}
		}

		validateCustomWhereAndWhereFields(configBean);
		validateOrderByFields(configBean);
		validateCacheSettings(configBean);
	}

	private void validateOrderByFields(SQLRetrievalConfigBean configBean) {
		if (configBean.getOrderBy() != null) {
			if (configBean.getOrderByTypes() != null && configBean.getOrderBy().size() != configBean.getOrderByTypes().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.SQLLookUp.ORDER_BY, SQLLookUp.ORDER_BY_TYPES);
			}
		}
	}

	private void validateCustomWhereAndWhereFields(SQLRetrievalConfigBean configBean) {
		if (configBean.getWhereFields() == null && configBean.getCustomWhere() == null) {
			exception.add(Message.EITHER_OF_THE_KEY_IS_MANDATORY, Operation.General.WHERE_FIELDS, Operation.General.CUSTOM_WHERE);
		} else if (configBean.getWhereFields() != null && configBean.getCustomWhere() != null) {
			exception.add(Message.EITHER_OF_THE_KEY_IS_MANDATORY, Operation.General.WHERE_FIELDS, Operation.General.CUSTOM_WHERE);
		} else if (configBean.getWhereFields() != null) {
			if (configBean.getWhereColumns() != null && configBean.getWhereColumns().size() != configBean.getWhereFields().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.WHERE_FIELDS, Operation.General.WHERE_COLUMNS);
			}
		} else if (StringUtils.isNotBlank(configBean.getCustomWhere()) && !configBean.getCacheType().equals(CacheType.NONE)) {
			exception.add(Message.INVALID_WHEN_OTHER_IS_NOT, Operation.General.CUSTOM_WHERE, Operation.General.CACHE_TYPE, CacheType.NONE.name());
		}
	}

	private void validateCacheSettings(SQLRetrievalConfigBean configBean) {
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

	private void setConfigBean(SQLRetrievalConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, InvalidConfigException {

		configBean.setSelectColumns(ConfigUtil.getArrayList(configs, Operation.General.SELECT_COLUMNS, exception));
		ArrayList<Integer> positions = ConfigUtil.getArrayListOfIntegers(configs, Operation.General.SELECT_FIELD_POSITIONS, exception);
		configBean.setSelectFieldPositions(positions != null && !positions.isEmpty() ? positions : null);
		configBean.setSelectFieldAliases(ConfigUtil.getArrayList(configs, Operation.General.SELECT_COLUMNS_AS_FIELDS, exception));
		configBean.setInsertValues(ConfigUtil.getArrayList(configs, Operation.General.INSERT_VALUES, exception));
		configBean.setInsertValueByFields(ConfigUtil.getArrayList(configs, Operation.General.INSERT_VALUE_BY_FIELDS, exception));
		configBean.setInsertValuesByFieldFormats(ConfigUtil.getArrayList(configs, Operation.General.INSERT_VALUE_BY_FIELD_FORMATS, exception));

		configBean.setWhereFields(ConfigUtil.getArrayList(configs, Operation.General.WHERE_FIELDS, exception));
		configBean.setWhereColumns(ConfigUtil.getArrayList(configs, Operation.General.WHERE_COLUMNS, exception));

		configBean.setOnZeroFetch(OnZeroFetchOperation.getInstance(ConfigUtil.getString(configs, Operation.General.ON_ZERO_FETCH)));
		configBean.setUseAIValue(ConfigUtil.getBoolean(configs, SQLNoSQL.USE_AI_VALUE, exception, false));

		configBean.setTableName(ConfigUtil.getString(configs, SQLNoSQL.TABLE));
		configBean.setName(ConfigUtil.getString(configs, General.NAME));
		configBean.setRdbmsConnection(ConfigUtil.getString(configs, SQLNoSQL.JDBC_URL), ConfigUtil.getString(configs, SQLNoSQL.DRIVER),
		        ConfigUtil.getString(configs, SQLNoSQL.USER_NAME), ConfigUtil.getString(configs, SQLNoSQL.PASSWORD));

		configBean.setCustomWhere(ConfigUtil.getString(configs, Operation.General.CUSTOM_WHERE));

		configBean.setOrderBy(ConfigUtil.getArrayList(configs, Operation.SQLLookUp.ORDER_BY, exception));
		configBean.setOrderByTypes(ConfigUtil.getArrayList(configs, SQLLookUp.ORDER_BY_TYPES, exception));

		configBean.setKeyFieldsCaseSensitive(ConfigUtil.getBoolean(configs, General.KEY_FIELDS_CASE_SENSITIVE, exception, false));

		setCacheSettings(configBean, configs);
	}

	private void setCacheSettings(SQLRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) {
		configBean.setCacheType(CacheType.getInstance(ConfigUtil.getString(configs, Operation.General.CACHE_TYPE)));
		configBean.setCacheMode(CacheMode.getInstance(ConfigUtil.getString(configs, Operation.General.CACHE_MODE)));
		configBean.setTimeToIdleSeconds(StringUtils.isEmpty(configs.get(Operation.General.CACHE_TIME_TO_IDLE)) ? null
		        : Long.parseLong(configs.get(Operation.General.CACHE_TIME_TO_IDLE)));
		configBean.setMaxElementsInMemory(StringUtils.isEmpty(configs.get(General.CACHE_MAX_ELEMENTS_IN_MEMORY)) ? null
		        : Integer.parseInt(configs.get(General.CACHE_MAX_ELEMENTS_IN_MEMORY)));
	}

	protected abstract void setSQLRetrievalConfigBean(SQLRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) throws InvalidConfigException;

	protected abstract void validateSQLRetrievalConfigBean(SQLRetrievalConfigBean configBean, LinkedHashMap<String, String> configs);
}

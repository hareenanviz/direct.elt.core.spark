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
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.RethinkDefault;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.RethinkLookUp;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.RethinkFetcherConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.RethinkLookUpConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.RethinkRetrievalConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class RethinkRetrievalValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public RethinkRetrievalValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {
		validateMandatoryFields(configs, SQLNoSQL.HOST, SQLNoSQL.TABLE);

		RethinkRetrievalConfigBean configBean = null;

		if (this instanceof RethinkLookUpValidator) {
			configBean = new RethinkLookUpConfigBean();
		} else {
			configBean = new RethinkFetcherConfigBean();
		}

		validateAndSetConfigBean(configBean, configs);

		return configBean;
	}

	private void validateAndSetConfigBean(RethinkRetrievalConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, UnsupportedException {
		setConfigBean(configBean, configs);
		setRetrievalConfigBean(configBean, configs);

		validateConfigBean(configBean, configs);
		validateRetrievalConfigBean(configBean, configs);
	}

	private void setConfigBean(RethinkRetrievalConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, UnsupportedException {
		ArrayList<String> selectFields = ConfigUtil.getArrayList(configs, Operation.General.SELECT_COLUMNS, exception);
		ArrayList<Class<?>> selectFieldDataTypes = ConfigUtil.getArrayListOfClass(configs, RethinkLookUp.SELECT_FIELD_TYPES, exception);
		ArrayList<String> selectFieldAliases = ConfigUtil.getArrayList(configs, Operation.General.SELECT_COLUMNS_AS_FIELDS, exception);
		ArrayList<String> insertValues = ConfigUtil.getArrayList(configs, Operation.General.INSERT_VALUES, exception);
		ArrayList<Integer> positions = ConfigUtil.getArrayListOfIntegers(configs, Operation.General.SELECT_FIELD_POSITIONS, exception);
		ArrayList<Integer> precisions = ConfigUtil.getArrayListOfIntegers(configs, General.DECIMAL_PRECISIONS, exception);
		ArrayList<Integer> scales = ConfigUtil.getArrayListOfIntegers(configs, General.DECIMAL_SCALES, exception);

		configBean.setTableName(ConfigUtil.getString(configs, SQLNoSQL.TABLE));
		configBean.setSelectFields(selectFields);
		configBean.setSelectFieldDateFormats(ConfigUtil.getArrayList(configs, RethinkLookUp.SELECT_FIELD_DATE_FORMATS, exception));
		configBean.setSelectFieldAliases(ConfigUtil.getArrayList(configs, Operation.General.SELECT_COLUMNS_AS_FIELDS, exception));
		configBean.setSelectFieldPositions(positions != null && !positions.isEmpty() ? positions : null);
		configBean.setEqFields(ConfigUtil.getArrayList(configs, RethinkLookUp.EQ_FIELDS, exception));
		configBean.setEqColumns(ConfigUtil.getArrayList(configs, RethinkLookUp.EQ_COLUMNS, exception));
		configBean.setLtFields(ConfigUtil.getArrayList(configs, RethinkLookUp.LT_FIELDS, exception));
		configBean.setLtColumns(ConfigUtil.getArrayList(configs, RethinkLookUp.LT_COLUMNS, exception));
		configBean.setGtFields(ConfigUtil.getArrayList(configs, RethinkLookUp.GT_FIELDS, exception));
		configBean.setGtColumns(ConfigUtil.getArrayList(configs, RethinkLookUp.GT_COLUMNS, exception));
		configBean.setInsertValues(insertValues);
		configBean.setPrecisions(precisions);
		configBean.setScales(scales);
		configBean.setOnZeroFetch(OnZeroFetchOperation.getInstance(ConfigUtil.getString(configs, Operation.General.ON_ZERO_FETCH)));

		setConnection(configBean, configs);

		configBean.setStructure(selectFieldAliases == null || selectFieldAliases.isEmpty() ? selectFields : selectFieldAliases, selectFieldDataTypes);

		setCacheSettings(configBean, configs);
	}

	private void setConnection(RethinkRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		ArrayList<String> host = ConfigUtil.getArrayList(configs, SQLNoSQL.HOST, exception);
		ArrayList<Integer> portNumber = ConfigUtil.getArrayListOfIntegers(configs, SQLNoSQL.PORT_NUMBER, exception);
		String dbName = ConfigUtil.getString(configs, SQLNoSQL.DB_NAME);
		String userName = ConfigUtil.getString(configs, SQLNoSQL.USER_NAME);
		String password = ConfigUtil.getString(configs, SQLNoSQL.PASSWORD);
		Long timeout = ConfigUtil.getLong(configs, SQLNoSQL.TIMEOUT, exception, RethinkDefault.TIMEOUT);

		configBean.setRethinkDBConnection(host, portNumber, dbName, userName, password, timeout);
	}

	private void setCacheSettings(RethinkRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) {
		configBean.setCacheType(CacheType.getInstance(ConfigUtil.getString(configs, Operation.General.CACHE_TYPE)));
		configBean.setCacheMode(CacheMode.getInstance(ConfigUtil.getString(configs, Operation.General.CACHE_MODE)));
		configBean.setTimeToIdleSeconds(StringUtils.isEmpty(configs.get(Operation.General.CACHE_TIME_TO_IDLE)) ? null
		        : Long.parseLong(configs.get(Operation.General.CACHE_TIME_TO_IDLE)));
		configBean.setMaxElementsInMemory(StringUtils.isEmpty(configs.get(General.CACHE_MAX_ELEMENTS_IN_MEMORY)) ? null
		        : Integer.parseInt(configs.get(General.CACHE_MAX_ELEMENTS_IN_MEMORY)));
	}

	private void validateConfigBean(RethinkRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) {
		validateMandatoryFields(configs, Operation.General.SELECT_COLUMNS, RethinkLookUp.SELECT_FIELD_TYPES);

		if (configBean.getSelectFields() != null) {
			if (configBean.getSelectFieldTypes() != null && configBean.getSelectFields().size() != configBean.getSelectFieldTypes().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.SELECT_COLUMNS, RethinkLookUp.SELECT_FIELD_TYPES);
			}

			if (configBean.getSelectFieldDateFormats() != null && configBean.getSelectFields().size() != configBean.getSelectFieldDateFormats().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.SELECT_COLUMNS, RethinkLookUp.SELECT_FIELD_DATE_FORMATS);
			}

			if (configBean.getSelectFieldPositions() != null && configBean.getSelectFields().size() != configBean.getSelectFieldPositions().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.SELECT_COLUMNS, Operation.General.SELECT_FIELD_POSITIONS);
			}

			if (configBean.getSelectFieldAliases() != null && configBean.getSelectFields().size() != configBean.getSelectFieldAliases().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.SELECT_COLUMNS, Operation.General.SELECT_COLUMNS_AS_FIELDS);
			}

			if (configBean.getPrecisions() != null && configBean.getPrecisions().size() != configBean.getSelectFields().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.SELECT_COLUMNS, General.DECIMAL_PRECISIONS);
			}

			if (configBean.getScales() != null && configBean.getScales().size() != configBean.getSelectFields().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.SELECT_COLUMNS, General.DECIMAL_SCALES);
			}

			validateOnZeroFetchAndInsertValues(configBean);
		}

		validateWhereFields(configBean);
		validateCacheSettings(configBean);
	}

	private void validateOnZeroFetchAndInsertValues(RethinkRetrievalConfigBean configBean) {
		if (configBean.getOnZeroFetch() == null) {
			exception.add(ValidationConstant.Message.KEY_IS_INVALID, Operation.General.ON_ZERO_FETCH);
		} else if (configBean.getOnZeroFetch().equals(OnZeroFetchOperation.INSERT)
		        && (configBean.getInsertValues() == null || configBean.getInsertValues().isEmpty())) {
			exception.add(ValidationConstant.Message.IS_MANDATORY_WHEN_PRESENT, Operation.General.INSERT_VALUES,
			        Operation.General.ON_ZERO_FETCH + " '" + configBean.getOnZeroFetch() + "'");
		} else if (configBean.getOnZeroFetch().equals(OnZeroFetchOperation.INSERT) && configBean.getInsertValues() != null
		        && configBean.getInsertValues().size() != configBean.getSelectFields().size()) {
			exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.SELECT_COLUMNS, Operation.General.INSERT_VALUES);
		} else if (!configBean.getOnZeroFetch().equals(OnZeroFetchOperation.INSERT) && configBean.getInsertValues() != null
		        && !configBean.getInsertValues().isEmpty()) {
			exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_PRECENT, Operation.General.INSERT_VALUES,
			        Operation.General.ON_ZERO_FETCH + " '" + configBean.getOnZeroFetch() + "'");
		}
	}

	private void validateWhereFields(RethinkRetrievalConfigBean configBean) {
		if ((configBean.getEqFields() == null || configBean.getEqFields().size() == 0)
		        && (configBean.getLtFields() == null || configBean.getLtFields().size() == 0)
		        && (configBean.getGtFields() == null || configBean.getGtFields().size() == 0)) {
			exception.add(ValidationConstant.Message.EITHER_OF_THE_THREE_IS_MANDATORY, RethinkLookUp.EQ_FIELDS, RethinkLookUp.LT_FIELDS,
			        RethinkLookUp.GT_FIELDS);
		} else {
			validateWhereFields(configBean.getEqFields(), configBean.getEqColumns(), RethinkLookUp.EQ_FIELDS, RethinkLookUp.EQ_COLUMNS);
			validateWhereFields(configBean.getLtFields(), configBean.getLtColumns(), RethinkLookUp.LT_FIELDS, RethinkLookUp.LT_COLUMNS);
			validateWhereFields(configBean.getGtFields(), configBean.getGtColumns(), RethinkLookUp.GT_FIELDS, RethinkLookUp.GT_COLUMNS);
		}
	}

	private void validateWhereFields(ArrayList<String> fields, ArrayList<String> columns, String fieldConstant, String columnConstant) {
		if (fields != null && columns != null && fields.size() != columns.size()) {
			exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, fieldConstant, columnConstant);
		}
	}

	private void validateCacheSettings(RethinkRetrievalConfigBean configBean) {
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

	protected abstract void validateRetrievalConfigBean(RethinkRetrievalConfigBean configBean, LinkedHashMap<String, String> configs);

	protected abstract void setRetrievalConfigBean(RethinkRetrievalConfigBean configBean, LinkedHashMap<String, String> configs);
}

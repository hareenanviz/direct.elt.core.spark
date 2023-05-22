package com.anvizent.elt.core.spark.sink.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.BatchType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink.SQLSink;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.DBCheckMode;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.constant.DBWriteMode;
import com.anvizent.elt.core.spark.constant.SQLUpdateType;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.sink.config.bean.DBConstantsConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;
import com.anvizent.elt.core.spark.validator.ErrorHandlerValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkValidator extends Validator implements ErrorHandlerValidator {

	private static final long serialVersionUID = 1L;

	public SQLSinkValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException {
		validateMandatoryFields(configs, SQLNoSQL.DRIVER, SQLNoSQL.JDBC_URL, SQLNoSQL.USER_NAME, SQLNoSQL.PASSWORD, SQLNoSQL.TABLE);

		SQLSinkConfigBean configBean = new SQLSinkConfigBean();
		validateAndSetConfigBean(configBean, configs);

		return configBean;
	}

	private void validateAndSetConfigBean(SQLSinkConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, InvalidConfigException {
		setConfigBean(configBean, configs);

		if (configBean.getDBInsertMode() != null) {
			if (!configBean.getDBCheckMode().equals(DBCheckMode.DB_QUERY) && !configBean.getDBInsertMode().equals(DBInsertMode.INSERT)
			        && (configBean.getKeyFields() == null || configBean.getKeyFields().isEmpty())) {
				exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, Sink.General.KEY_FIELDS);
			} else if (configBean.getKeyFields() != null && configBean.getKeyColumns() != null
			        && configBean.getKeyFields().size() != configBean.getKeyColumns().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Sink.General.KEY_FIELDS, Sink.General.KEY_COLUMNS);
			} else if (configBean.getKeyFields() != null && configBean.getKeyColumns() != null) {
				if (configBean.getMetaDataFields() != null && !configBean.getMetaDataFields().isEmpty()
				        && (configBean.getDBInsertMode().equals(DBInsertMode.UPDATE) || configBean.getDBInsertMode().equals(DBInsertMode.UPSERT))) {

					ArrayList<String> keyFields = configBean.getKeyColumns() == null || configBean.getKeyColumns().isEmpty() ? configBean.getKeyFields()
					        : configBean.getKeyColumns();
					for (String metaDataField : configBean.getMetaDataFields()) {
						if (keyFields.contains(metaDataField)) {
							exception.add(ValidationConstant.Message.KEY_IS_PRESENT_IN_OTHER, Sink.General.META_DATA_FIELDS, Sink.General.KEY_FIELDS);
							break;
						}
					}
				}
			}
			if (configBean.getColumnsDifferToFields() != null && !configBean.getColumnsDifferToFields().isEmpty()) {
				if (configBean.getFieldsDifferToColumns() == null || configBean.getFieldsDifferToColumns().isEmpty()) {
					exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, Sink.General.FIELD_NAMES_DIFFER_TO_COLUMNS);
				} else if (configBean.getColumnsDifferToFields().size() != configBean.getFieldsDifferToColumns().size()) {
					exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Sink.General.FIELD_NAMES_DIFFER_TO_COLUMNS,
					        Sink.General.COLUMN_NAMES_DIFFER_TO_FIELDS);
				}
			}

			validateDBWriteAndInsertMode(configBean);
			validateDeleteIndicatorField(configBean);
			validateDBSinkConstants(configBean);
			validateBatchSize(configBean);
			validatePrefetch(configBean, configs);

			if (configBean.getDBInsertMode().equals(DBInsertMode.INSERT) && configBean.isAlwaysUpdate()) {
				exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_IS, Sink.General.ALWAYS_UPDATE, Sink.General.INSERT_MODE, DBInsertMode.INSERT);
			}

			if (configBean.getDBInsertMode().equals(DBInsertMode.INSERT) && configBean.getChecksumField() != null && !configBean.getChecksumField().isEmpty()) {
				exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_IS, Sink.General.CHECK_SUM_FIELD, Sink.General.INSERT_MODE, DBInsertMode.INSERT);
			}

			if (configBean.isAlwaysUpdate() && configBean.getChecksumField() != null && !configBean.getChecksumField().isEmpty()) {
				exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_IS, Sink.General.CHECK_SUM_FIELD, Sink.General.ALWAYS_UPDATE, true);
			}
		} else {
			exception.add(ValidationConstant.Message.KEY_IS_INVALID, Sink.General.INSERT_MODE);
		}
	}

	private void validateDeleteIndicatorField(SQLSinkConfigBean configBean) {
		if (StringUtils.isNotBlank(configBean.getDeleteIndicatorField())) {
			DBWriteMode dbWriteMode = configBean.getDBWriteMode();

			if (!dbWriteMode.equals(DBWriteMode.APPEND)) {
				exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_IS, Sink.General.DELETE_INDICATOR_FIELD, Sink.General.WRITE_MODE,
				        dbWriteMode.name());
			}
		}
	}

	private void validatePrefetch(SQLSinkConfigBean configBean, LinkedHashMap<String, String> configs) {
		String checkMode = ConfigUtil.getString(configs, Sink.SQLSink.DB_CHECK_MODE);
		if (configBean.getDBCheckMode() == null && StringUtils.isNotBlank(checkMode)) {
			exception.add(ValidationConstant.Message.INVALID_VALUE_FOR, checkMode, Sink.SQLSink.DB_CHECK_MODE);
		}

		if (!configBean.getDBCheckMode().equals(DBCheckMode.PREFECTH)) {
			if (configBean.getPrefetchBatchSize() != null) {
				exception.add(ValidationConstant.Message.INVALID_FOR_OTHER, Sink.SQLSink.PREFETCH_BATCH_SIZE, configBean.getPrefetchBatchSize(),
				        Sink.SQLSink.DB_CHECK_MODE, configBean.getDBCheckMode().name());
			}

			if (configBean.getMaxElementsInMemory() != null) {
				exception.add(ValidationConstant.Message.INVALID_FOR_OTHER, General.CACHE_MAX_ELEMENTS_IN_MEMORY, configBean.getMaxElementsInMemory(),
				        Sink.SQLSink.DB_CHECK_MODE, configBean.getDBCheckMode().name());
			}

			if (configBean.isRemoveOnceUsed()) {
				exception.add(ValidationConstant.Message.INVALID_FOR_OTHER, Sink.SQLSink.REMOVE_ONCE_USED, configBean.isRemoveOnceUsed(),
				        Sink.SQLSink.DB_CHECK_MODE, configBean.getDBCheckMode().name());
			}
		}

		if (!configBean.getDBCheckMode().equals(DBCheckMode.REGULAR) && configBean.getDBInsertMode().equals(DBInsertMode.INSERT)) {
			exception.add(ValidationConstant.Message.INVALID_FOR_OTHER, Sink.General.INSERT_MODE, configBean.getDBInsertMode().name(),
			        Sink.SQLSink.DB_CHECK_MODE, configBean.getDBCheckMode().name());
		}

		if (configBean.getDBCheckMode().equals(DBCheckMode.DB_QUERY)) {
			if (!configBean.getDBInsertMode().equals(DBInsertMode.UPSERT) && !configBean.getDBInsertMode().equals(DBInsertMode.INSERT_IF_NOT_EXISTS)) {
				exception.add(ValidationConstant.Message.INVALID_FOR_OTHER, Sink.General.INSERT_MODE, configBean.getDBInsertMode().name(),
				        Sink.SQLSink.DB_CHECK_MODE, configBean.getDBCheckMode().name());
			}

			if (!CollectionUtils.isEmpty(configBean.getKeyFields())) {
				exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_IS, Sink.General.KEY_FIELDS, Sink.SQLSink.DB_CHECK_MODE,
				        configBean.getDBCheckMode().name());
			}

			if (!CollectionUtils.isEmpty(configBean.getKeyColumns())) {
				exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_IS, Sink.General.KEY_COLUMNS, Sink.SQLSink.DB_CHECK_MODE,
				        configBean.getDBCheckMode().name());
			}
		}

		if (configBean.getDBCheckMode().equals(DBCheckMode.PREFECTH)) {
			if (configBean.getPrefetchBatchSize() == null) {
				exception.add(ValidationConstant.Message.IS_MANDATORY_FOR_VALUE, Sink.SQLSink.PREFETCH_BATCH_SIZE, Sink.SQLSink.DB_CHECK_MODE,
				        configBean.getDBCheckMode().name());
			}

			if (configBean.getPrefetchBatchSize() != -1 && configBean.getPrefetchBatchSize() <= 0) {
				exception.add(ValidationConstant.Message.INVALID_VALUE_FOR, Sink.SQLSink.PREFETCH_BATCH_SIZE, configBean.getPrefetchBatchSize());
			}
		}
	}

	private void validateBatchSize(SQLSinkConfigBean configBean) {
		if (configBean.getBatchType() == null) {
			exception.add(ValidationConstant.Message.KEY_IS_INVALID, Sink.General.BATCH_TYPE);
		} else if (configBean.getBatchType().equals(BatchType.ALL) || configBean.getBatchType().equals(BatchType.NONE)) {
			if (configBean.getBatchSize() != 0) {
				exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_PRECENT, Sink.General.BATCH_SIZE,
				        Sink.General.BATCH_TYPE + ": '" + configBean.getBatchType() + "'");
			}
		} else {
			if (configBean.getBatchSize() <= 0) {
				exception.add(ValidationConstant.Message.INVALID_VALUE_FOR, configBean.getBatchSize(), Sink.General.BATCH_SIZE);
			}
		}
	}

	private void validateDBSinkConstants(SQLSinkConfigBean configBean) {
		validateDBConstants(configBean.getConstantsConfigBean(), configBean.getDBInsertMode(), configBean.getDBWriteMode(), SQLSink.CONSTANT_COLUMNS,
		        SQLSink.CONSTANT_STORE_TYPES, SQLSink.CONSTANT_STORE_VALUES, SQLSink.CONSTANT_INDEXES, DBInsertMode.UPSERT, false);

		validateDBConstants(configBean.getInsertConstantsConfigBean(), configBean.getDBInsertMode(), configBean.getDBWriteMode(),
		        SQLSink.INSERT_CONSTANT_COLUMNS, SQLSink.INSERT_CONSTANT_STORE_TYPES, SQLSink.INSERT_CONSTANT_STORE_VALUES, SQLSink.INSERT_CONSTANT_INDEXES,
		        DBInsertMode.UPSERT, true);

		validateDBConstants(configBean.getUpdateConstantsConfigBean(), configBean.getDBInsertMode(), configBean.getDBWriteMode(),
		        SQLSink.UPDATE_CONSTANT_COLUMNS, SQLSink.UPDATE_CONSTANT_STORE_TYPES, SQLSink.UPDATE_CONSTANT_STORE_VALUES, SQLSink.UPDATE_CONSTANT_INDEXES,
		        DBInsertMode.UPSERT, true);
	}

	private void validateDBConstants(DBConstantsConfigBean constantsConfigBean, DBInsertMode configDBInsertMode, DBWriteMode configDBWritetMode,
	        String fieldsConfig, String typesConfig, String valuesConfig, String indexesConfig, DBInsertMode dbInsertMode, boolean applicable) {
		validateDBConstants(configDBInsertMode, configDBWritetMode, constantsConfigBean.getColumns(), fieldsConfig, constantsConfigBean.getTypes(), typesConfig,
		        constantsConfigBean.getValues(), valuesConfig, constantsConfigBean.getIndexes(), indexesConfig, dbInsertMode, applicable);
	}

	private void validateDBConstants(DBInsertMode configDBInsertMode, DBWriteMode configDBWritetMode, ArrayList<String> fields, String fieldsConfig,
	        ArrayList<String> types, String typesConfig, ArrayList<String> values, String valuesConfig, ArrayList<Integer> indexes, String indexesConfig,
	        DBInsertMode dbInsertMode, boolean applicable) {
		if (fields != null && !fields.isEmpty()) {

			if ((!applicable && !configDBInsertMode.equals(dbInsertMode)) || (applicable && configDBInsertMode.equals(dbInsertMode))) {

				if (values == null) {
					exception.add(ValidationConstant.Message.IS_MANDATORY_WHEN_PRESENT, valuesConfig, fieldsConfig);
				} else if (fields.size() != values.size()) {
					exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, fieldsConfig, valuesConfig);
				}

				if ((types == null || types.isEmpty()) && configDBWritetMode.equals(DBWriteMode.OVERWRITE)) {
					exception.add(ValidationConstant.Message.IS_MANDATORY_WHEN_PRESENT, typesConfig, fieldsConfig);
				} else if ((types != null && !types.isEmpty()) && fields.size() != types.size()) {
					exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, fieldsConfig, typesConfig);
				}

				if (indexes != null && fields.size() != indexes.size()) {
					exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, fieldsConfig, indexesConfig);
				}
			} else {
				exception.add(ValidationConstant.Message.UNSUPPORTED_CONFIGS_FOR_DB_SINK_MODE, fieldsConfig, valuesConfig, typesConfig, typesConfig,
				        configDBInsertMode.name());
			}

		} else {
			if (values != null && !values.isEmpty()) {
				exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_NOT_PRECENT, valuesConfig, fieldsConfig);
			}

			if (types != null && !types.isEmpty()) {
				exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_NOT_PRECENT, typesConfig, fieldsConfig);
			}

			if (indexes != null && !indexes.isEmpty()) {
				exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_NOT_PRECENT, indexesConfig, fieldsConfig);
			}
		}
	}

	private void validateDBWriteAndInsertMode(SQLSinkConfigBean configBean) {
		DBInsertMode dbInsertMode = configBean.getDBInsertMode();
		DBWriteMode dbWriteMode = configBean.getDBWriteMode();

		if (dbWriteMode == null) {
			exception.add(ValidationConstant.Message.KEY_IS_INVALID, Sink.General.WRITE_MODE);
		} else if (dbInsertMode == null) {
			exception.add(ValidationConstant.Message.KEY_IS_INVALID, Sink.General.INSERT_MODE);
		} else {
			if (dbWriteMode.equals(DBWriteMode.OVERWRITE) && !dbInsertMode.equals(DBInsertMode.INSERT)) {
				exception.add(ValidationConstant.Message.INVALID_FOR_OTHER, Sink.General.WRITE_MODE, dbWriteMode.name(), Sink.General.INSERT_MODE,
				        dbInsertMode.name());
			} else if (dbWriteMode.equals(DBWriteMode.TRUNCATE) && !dbInsertMode.equals(DBInsertMode.INSERT)) {
				exception.add(ValidationConstant.Message.INVALID_FOR_OTHER, Sink.General.WRITE_MODE, dbWriteMode.name(), Sink.General.INSERT_MODE,
				        dbInsertMode.name());
			} else if (dbWriteMode.equals(DBWriteMode.IGNORE) && !dbInsertMode.equals(DBInsertMode.INSERT)) {
				exception.add(ValidationConstant.Message.INVALID_FOR_OTHER, Sink.General.WRITE_MODE, dbWriteMode.name(), Sink.General.INSERT_MODE,
				        dbInsertMode.name());
			} else if (dbWriteMode.equals(DBWriteMode.FAIL) && !dbInsertMode.equals(DBInsertMode.INSERT)) {
				exception.add(ValidationConstant.Message.INVALID_FOR_OTHER, Sink.General.WRITE_MODE, dbWriteMode.name(), Sink.General.INSERT_MODE,
				        dbInsertMode.name());
			}
		}
	}

	private void setConfigBean(SQLSinkConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException, InvalidConfigException {
		setConnection(configBean, configs);
		configBean.setTableName(ConfigUtil.getString(configs, SQLNoSQL.TABLE));
		configBean.setDBWriteMode(DBWriteMode.getInstance(ConfigUtil.getString(configs, Sink.General.WRITE_MODE)));
		configBean.setDBInsertMode(DBInsertMode.getInstance(ConfigUtil.getString(configs, Sink.General.INSERT_MODE)));
		configBean.setDeleteIndicatorField(ConfigUtil.getString(configs, Sink.General.DELETE_INDICATOR_FIELD));

		setPrefetchInfo(configBean, configs);
		setFileds(configBean, configs);
		setRetryConfigs(configBean, configs);
		setBatchConfigs(configBean, configs);

		setDBConstants(configBean, configs);
		configBean.setAlwaysUpdate(ConfigUtil.getBoolean(configs, Sink.General.ALWAYS_UPDATE, exception, false));
		configBean.setUpdateUsing(SQLUpdateType.getInstance(ConfigUtil.getString(configs, SQLNoSQL.UPDATE_USING), SQLUpdateType.BATCH));
		configBean.setChecksumField(ConfigUtil.getString(configs, Sink.General.CHECK_SUM_FIELD));

		configBean.setOnConnectRunQuery(ConfigUtil.getString(configs, Sink.SQLSink.ON_CONNECT_RUN_QUERY));
		configBean.setBeforeComponentRunQuery(ConfigUtil.getString(configs, Sink.SQLSink.BEFORE_COMPONENT_RUN_QUERY));
		configBean.setAfterComponentSuccessRunQuery(ConfigUtil.getString(configs, Sink.SQLSink.AFTER_COMPONENT_SUCCESS_RUN_QUERY));
	}

	private void setPrefetchInfo(SQLSinkConfigBean configBean, LinkedHashMap<String, String> configs) {
		configBean.setDBCheckMode(DBCheckMode.getInstance(ConfigUtil.getString(configs, Sink.SQLSink.DB_CHECK_MODE)));

		configBean.setPrefetchBatchSize(ConfigUtil.getInteger(configs, Sink.SQLSink.PREFETCH_BATCH_SIZE, exception));
		configBean.setRemoveOnceUsed(ConfigUtil.getBoolean(configs, Sink.SQLSink.REMOVE_ONCE_USED, exception, false));
		configBean.setMaxElementsInMemory(ConfigUtil.getInteger(configs, General.CACHE_MAX_ELEMENTS_IN_MEMORY, exception));
	}

	private void setFileds(SQLSinkConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		configBean.setKeyFields(ConfigUtil.getArrayList(configs, Sink.General.KEY_FIELDS, exception));
		configBean.setKeyColumns(ConfigUtil.getArrayList(configs, Sink.General.KEY_COLUMNS, exception));

		configBean.setFieldsDifferToColumns(ConfigUtil.getArrayList(configs, Sink.General.FIELD_NAMES_DIFFER_TO_COLUMNS, exception));
		configBean.setColumnsDifferToFields(ConfigUtil.getArrayList(configs, Sink.General.COLUMN_NAMES_DIFFER_TO_FIELDS, exception));

		configBean.setMetaDataFields(ConfigUtil.getArrayList(configs, Sink.General.META_DATA_FIELDS, exception));
		configBean.setKeyFieldsCaseSensitive(ConfigUtil.getBoolean(configs, SQLSink.KEY_FIELDS_CASE_SENSITIVE, exception));
	}

	private void setRetryConfigs(SQLSinkConfigBean configBean, LinkedHashMap<String, String> configs) {
		configBean.setInitRetryConfigBean(ConfigUtil.getInteger(configs, Sink.General.INIT_MAX_RETRY_COUNT, exception, General.DEFAULT_MAX_RETRY_COUNT),
		        ConfigUtil.getLong(configs, Sink.General.INIT_RETRY_DELAY, exception, General.DEFAULT_RETRY_DELAY));

		configBean.setDestroyRetryConfigBean(ConfigUtil.getInteger(configs, Sink.General.DESTROY_MAX_RETRY_COUNT, exception, General.DEFAULT_MAX_RETRY_COUNT),
		        ConfigUtil.getLong(configs, Sink.General.DESTROY_RETRY_DELAY, exception, General.DEFAULT_RETRY_DELAY));
	}

	private void setBatchConfigs(SQLSinkConfigBean configBean, LinkedHashMap<String, String> configs) {
		configBean.setBatchType(BatchType.getInstance(ConfigUtil.getString(configs, Sink.General.BATCH_TYPE)));
		configBean.setBatchSize(ConfigUtil.getLong(configs, Sink.General.BATCH_SIZE, exception, 0L));
	}

	private void setDBConstants(SQLSinkConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		configBean.setConstantsConfigBean(
		        getDBConstants(configs, SQLSink.CONSTANT_COLUMNS, SQLSink.CONSTANT_STORE_TYPES, SQLSink.CONSTANT_STORE_VALUES, SQLSink.CONSTANT_INDEXES));

		configBean.setInsertConstantsConfigBean(getDBConstants(configs, SQLSink.INSERT_CONSTANT_COLUMNS, SQLSink.INSERT_CONSTANT_STORE_TYPES,
		        SQLSink.INSERT_CONSTANT_STORE_VALUES, SQLSink.INSERT_CONSTANT_INDEXES));

		configBean.setUpdateConstantsConfigBean(getDBConstants(configs, SQLSink.UPDATE_CONSTANT_COLUMNS, SQLSink.UPDATE_CONSTANT_STORE_TYPES,
		        SQLSink.UPDATE_CONSTANT_STORE_VALUES, SQLSink.UPDATE_CONSTANT_INDEXES));
	}

	private DBConstantsConfigBean getDBConstants(LinkedHashMap<String, String> configs, String fieldsConfig, String typesConfig, String valuesConfig,
	        String indexesConfig) throws ImproperValidationException {

		DBConstantsConfigBean constantsConfigBean = new DBConstantsConfigBean();

		constantsConfigBean.setColumns(ConfigUtil.getArrayList(configs, fieldsConfig, exception));
		constantsConfigBean.setTypes(ConfigUtil.getArrayList(configs, typesConfig, exception));
		constantsConfigBean.setValues(ConfigUtil.getArrayList(configs, valuesConfig, exception));
		constantsConfigBean.setIndexes(ConfigUtil.getArrayListOfIntegers(configs, indexesConfig, exception));

		return constantsConfigBean;
	}

	private void setConnection(SQLSinkConfigBean configBean, LinkedHashMap<String, String> configs) {
		String jdbcURL = ConfigUtil.getString(configs, SQLNoSQL.JDBC_URL);
		String driver = ConfigUtil.getString(configs, SQLNoSQL.DRIVER);
		String userName = ConfigUtil.getString(configs, SQLNoSQL.USER_NAME);
		String password = ConfigUtil.getString(configs, SQLNoSQL.PASSWORD);

		configBean.setRdbmsConnection(jdbcURL, driver, userName, password);
	}

	@Override
	public ConfigBean validateAndSetErrorHandler(LinkedHashMap<String, String> configs) throws ImproperValidationException, InvalidConfigException {
		SQLSinkConfigBean sqlSinkConfigBean = new SQLSinkConfigBean();

		setConnection(sqlSinkConfigBean, configs);
		sqlSinkConfigBean.setTableName(ConfigUtil.getString(configs, SQLNoSQL.TABLE));
		sqlSinkConfigBean.setDBWriteMode(DBWriteMode.APPEND);
		sqlSinkConfigBean.setDBInsertMode(DBInsertMode.INSERT);
		setRetryConfigs(sqlSinkConfigBean, configs);
		setDBConstants(sqlSinkConfigBean, configs);

		validateErrorHandlerStore(sqlSinkConfigBean, configs);
		sqlSinkConfigBean.setName(ConfigUtil.getString(configs, General.NAME));

		return sqlSinkConfigBean;
	}

	private SQLSinkConfigBean validateErrorHandlerStore(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, String> configs)
	        throws InvalidConfigException {
		validateConnectionDetails(sqlSinkConfigBean);
		validateDBWriteAndInsertMode(sqlSinkConfigBean);
		validateDBConstants(sqlSinkConfigBean.getConstantsConfigBean(), sqlSinkConfigBean.getDBInsertMode(), sqlSinkConfigBean.getDBWriteMode(),
		        SQLSink.CONSTANT_COLUMNS, SQLSink.CONSTANT_STORE_TYPES, SQLSink.CONSTANT_STORE_VALUES, SQLSink.CONSTANT_INDEXES, DBInsertMode.UPSERT, false);

		if (exception.getNumberOfExceptions() > 0) {
			throw exception;
		} else {
			return sqlSinkConfigBean;
		}
	}

	private void validateConnectionDetails(SQLSinkConfigBean sqlSinkConfigBean) {
		if (sqlSinkConfigBean.getRdbmsConnection().getDriver() == null || sqlSinkConfigBean.getRdbmsConnection().getDriver().isEmpty()) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, SQLNoSQL.DRIVER);
		}
		if (sqlSinkConfigBean.getRdbmsConnection().getJdbcURL() == null || sqlSinkConfigBean.getRdbmsConnection().getJdbcURL().isEmpty()) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, SQLNoSQL.JDBC_URL);
		}
		if (sqlSinkConfigBean.getRdbmsConnection().getUserName() == null || sqlSinkConfigBean.getRdbmsConnection().getUserName().isEmpty()) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, SQLNoSQL.USER_NAME);
		}
		if (sqlSinkConfigBean.getRdbmsConnection().getPassword() == null || sqlSinkConfigBean.getRdbmsConnection().getPassword().isEmpty()) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, SQLNoSQL.PASSWORD);
		}
		if (sqlSinkConfigBean.getTableName() == null || sqlSinkConfigBean.getTableName().isEmpty()) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, SQLNoSQL.TABLE);
		}
	}

	@Override
	public void replaceWithComponentSpecific(ErrorHandlerSink errorHandlerSink, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, InvalidConfigException {
		SQLSinkConfigBean sinkConfigBean = (SQLSinkConfigBean) errorHandlerSink;

		if (!ConfigUtil.isAllEmpty(configs, SQLNoSQL.DRIVER, SQLNoSQL.JDBC_URL, SQLNoSQL.USER_NAME, SQLNoSQL.PASSWORD)) {
			String driver = configs.containsKey(SQLNoSQL.DRIVER) ? ConfigUtil.getString(configs, SQLNoSQL.DRIVER)
			        : sinkConfigBean.getRdbmsConnection().getDriver();
			String jdbcURL = configs.containsKey(SQLNoSQL.JDBC_URL) ? ConfigUtil.getString(configs, SQLNoSQL.JDBC_URL)
			        : sinkConfigBean.getRdbmsConnection().getJdbcURL();
			String userName = configs.containsKey(SQLNoSQL.USER_NAME) ? ConfigUtil.getString(configs, SQLNoSQL.USER_NAME)
			        : sinkConfigBean.getRdbmsConnection().getUserName();
			String password = configs.containsKey(SQLNoSQL.PASSWORD) ? ConfigUtil.getString(configs, SQLNoSQL.PASSWORD)
			        : sinkConfigBean.getRdbmsConnection().getPassword();

			sinkConfigBean.setRdbmsConnection(jdbcURL, driver, userName, password);
		}

		if (!ConfigUtil.isAllEmpty(configs, SQLSink.CONSTANT_COLUMNS, SQLSink.CONSTANT_STORE_TYPES, SQLSink.CONSTANT_STORE_VALUES, SQLSink.CONSTANT_INDEXES)) {
			ArrayList<String> constantsFields = configs.containsKey(SQLSink.CONSTANT_COLUMNS)
			        ? ConfigUtil.getArrayList(configs, SQLSink.CONSTANT_COLUMNS, exception)
			        : sinkConfigBean.getConstantsConfigBean().getColumns();
			ArrayList<String> constantsTypes = configs.containsKey(SQLSink.CONSTANT_STORE_TYPES)
			        ? ConfigUtil.getArrayList(configs, SQLSink.CONSTANT_STORE_TYPES, exception)
			        : sinkConfigBean.getConstantsConfigBean().getTypes();
			ArrayList<String> constantsValues = configs.containsKey(SQLSink.CONSTANT_STORE_VALUES)
			        ? ConfigUtil.getArrayList(configs, SQLSink.CONSTANT_STORE_VALUES, exception)
			        : sinkConfigBean.getConstantsConfigBean().getValues();
			ArrayList<Integer> constantsIndexes = configs.containsKey(SQLSink.CONSTANT_INDEXES)
			        ? ConfigUtil.getArrayListOfIntegers(configs, SQLSink.CONSTANT_INDEXES, exception)
			        : sinkConfigBean.getConstantsConfigBean().getIndexes();

			DBConstantsConfigBean dbConstantsConfigBean = new DBConstantsConfigBean();
			dbConstantsConfigBean.setColumns(constantsFields);
			dbConstantsConfigBean.setTypes(constantsTypes);
			dbConstantsConfigBean.setValues(constantsValues);
			dbConstantsConfigBean.setIndexes(constantsIndexes);

			sinkConfigBean.setConstantsConfigBean(dbConstantsConfigBean);
		}

		sinkConfigBean.setTableName(configs.containsKey(SQLNoSQL.TABLE) ? ConfigUtil.getString(configs, SQLNoSQL.TABLE) : sinkConfigBean.getTableName());
		sinkConfigBean.setDBInsertMode(configs.containsKey(Sink.General.INSERT_MODE)
		        ? DBInsertMode.getInstance(ConfigUtil.getString(configs, Sink.General.INSERT_MODE), DBInsertMode.INSERT)
		        : sinkConfigBean.getDBInsertMode());
		sinkConfigBean.setDBWriteMode(configs.containsKey(Sink.General.WRITE_MODE)
		        ? DBWriteMode.getInstance(ConfigUtil.getString(configs, Sink.General.WRITE_MODE), DBWriteMode.APPEND)
		        : sinkConfigBean.getDBWriteMode());

		validateErrorHandlerStore(sinkConfigBean, configs);
	}

}

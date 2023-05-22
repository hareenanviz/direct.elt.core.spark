package com.anvizent.elt.core.spark.sink.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.commons.constants.Constants.ELTConstants;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.connection.RethinkDBConnection;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.BatchType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.RethinkDefault;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink.RethinkSink;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.constant.DBWriteMode;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.sink.config.bean.NoSQLConstantsConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.RethinkDBSinkConfigBean;
import com.anvizent.elt.core.spark.validator.ErrorHandlerValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class RethinkDBSinkValidator extends Validator implements ErrorHandlerValidator {

	private static final long serialVersionUID = 1L;

	public RethinkDBSinkValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException {
		validateMandatoryFields(configs, SQLNoSQL.HOST, SQLNoSQL.TABLE);

		RethinkDBSinkConfigBean configBean = new RethinkDBSinkConfigBean();
		validateAndSetConfigBean(configBean, configs, configAndMappingConfigBeans);

		return configBean;
	}

	private void validateAndSetConfigBean(RethinkDBSinkConfigBean configBean, LinkedHashMap<String, String> configs,
	        ConfigAndMappingConfigBeans configAndMappingConfigBeans) throws ImproperValidationException, InvalidConfigException {
		setErrorHandlerStore(configBean, configs);
		setConfigBean(configBean, configs);

		if (configBean.getDBInsertMode() != null) {

			if (!configBean.getDBInsertMode().equals(DBInsertMode.INSERT) && (configBean.getKeyFields() == null || configBean.getKeyFields().isEmpty())) {
				exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, Sink.General.KEY_FIELDS);
			} else if (configBean.getKeyFields() != null && !configBean.getKeyFields().isEmpty() && configBean.getKeyColumns() != null
			        && !configBean.getKeyColumns().isEmpty() && configBean.getKeyFields().size() != configBean.getKeyColumns().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Sink.General.KEY_FIELDS, Sink.General.KEY_COLUMNS);
			} else if (configBean.getKeyFields() != null && !configBean.getKeyFields().isEmpty() && configBean.getKeyColumns() != null
			        && !configBean.getKeyColumns().isEmpty()) {
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
			validateDBConstants(configBean);
			validateBatchTypeAndSize(configBean, configAndMappingConfigBeans);
		} else {
			exception.add(ValidationConstant.Message.KEY_IS_INVALID, Sink.General.INSERT_MODE);
		}
	}

	private void validateBatchTypeAndSize(RethinkDBSinkConfigBean configBean, ConfigAndMappingConfigBeans configAndMappingConfigBeans) {
		if (configBean.getBatchType() == null) {
			exception.add(ValidationConstant.Message.KEY_IS_INVALID, Sink.General.BATCH_TYPE);
		} else if (!configBean.getBatchType().equals(BatchType.NONE) && configAndMappingConfigBeans.getErrorHandlerSink() != null) {
			exception.add(ValidationConstant.Message.INVALID_TO_SPECIFY_EH_FOR_VALUE_OF, Sink.General.BATCH_TYPE, configBean.getBatchType());
		} else if (configBean.getBatchType().equals(BatchType.ALL) || configBean.getBatchType().equals(BatchType.NONE)) {
			if (configBean.getBatchSize() != 0) {
				exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_PRECENT, Sink.General.BATCH_SIZE,
				        Sink.General.BATCH_SIZE + ": '" + configBean.getBatchType() + "'");
			}
		} else {
			if (configBean.getBatchSize() <= 0) {
				exception.add(ValidationConstant.Message.INVALID_VALUE_FOR, configBean.getBatchSize(), Sink.General.BATCH_SIZE);
			}
		}
	}

	private void validateDBConstants(RethinkDBSinkConfigBean configBean) {
		validateDBConstants(configBean.getConstantsConfigBean(), configBean.getDBInsertMode(), RethinkSink.CONSTANT_FIELDS, RethinkSink.CONSTANT_TYPES,
		        RethinkSink.CONSTANT_VALUES, RethinkSink.LITERAL_CONSTANT_FIELDS, RethinkSink.LITERAL_CONSTANT_TYPES, RethinkSink.LITERAL_CONSTANT_VALUES,
		        RethinkSink.LITERAL_CONSTANT_DATE_FORMATS, DBInsertMode.UPSERT, false);

		validateDBConstants(configBean.getInsertConstantsConfigBean(), configBean.getDBInsertMode(), RethinkSink.INSERT_CONSTANT_FIELDS,
		        RethinkSink.INSERT_CONSTANT_TYPES, RethinkSink.INSERT_CONSTANT_VALUES, RethinkSink.INSERT_LITERAL_CONSTANT_FIELDS,
		        RethinkSink.INSERT_LITERAL_CONSTANT_TYPES, RethinkSink.INSERT_LITERAL_CONSTANT_VALUES, RethinkSink.INSERT_LITERAL_CONSTANT_DATE_FORMATS,
		        DBInsertMode.UPSERT, true);

		validateDBConstants(configBean.getUpdateConstantsConfigBean(), configBean.getDBInsertMode(), RethinkSink.UPDATE_CONSTANT_FIELDS,
		        RethinkSink.UPDATE_CONSTANT_TYPES, RethinkSink.UPDATE_CONSTANT_VALUES, RethinkSink.UPDATE_LITERAL_CONSTANT_FIELDS,
		        RethinkSink.UPDATE_LITERAL_CONSTANT_TYPES, RethinkSink.UPDATE_LITERAL_CONSTANT_VALUES, RethinkSink.UPDATE_LITERAL_CONSTANT_DATE_FORMATS,
		        DBInsertMode.UPSERT, true);
	}

	private void validateDBConstants(NoSQLConstantsConfigBean constantsConfigBean, DBInsertMode configDBInsertMode, String fieldsConfig, String typesConfig,
	        String valuesConfig, String literalFieldsConfig, String literalTypesConfig, String literalValuesConfig, String literalDateFormatsConfig,
	        DBInsertMode dbInsertMode, boolean applicable) {

		validateDBConstants(configDBInsertMode, constantsConfigBean.getFields(), fieldsConfig, constantsConfigBean.getTypes(), typesConfig,
		        constantsConfigBean.getValues(), valuesConfig, null, null, dbInsertMode, applicable);

		validateDBConstants(configDBInsertMode, constantsConfigBean.getLiteralFields(), literalFieldsConfig, constantsConfigBean.getLiteralTypes(),
		        literalTypesConfig, constantsConfigBean.getLiteralValues(), literalValuesConfig, constantsConfigBean.getDateFormats(), literalDateFormatsConfig,
		        dbInsertMode, applicable);
	}

	private void validateDBConstants(DBInsertMode configDBInsertMode, ArrayList<String> fields, String fieldsConfig, ArrayList<Class<?>> types,
	        String typesConfig, ArrayList<String> values, String valuesConfig, ArrayList<String> dateFormats, String dateFormatsConfig,
	        DBInsertMode dbInsertMode, boolean applicable) {
		if (fields != null && !fields.isEmpty()) {
			if ((applicable && configDBInsertMode.equals(dbInsertMode)) || (!applicable && !configDBInsertMode.equals(dbInsertMode))) {
				if (values == null) {
					exception.add(ValidationConstant.Message.IS_MANDATORY_WHEN_PRESENT, valuesConfig, fieldsConfig);
				} else if (fields.size() != values.size()) {
					exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, fieldsConfig, valuesConfig);
				}

				if (types == null || types.isEmpty()) {
					exception.add(ValidationConstant.Message.IS_MANDATORY_WHEN_PRESENT, typesConfig, fieldsConfig);
				} else if (types != null && fields.size() != types.size()) {
					exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, fieldsConfig, typesConfig);
				}

				if ((dateFormats != null && !dateFormats.isEmpty()) && fields.size() != dateFormats.size()) {
					exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, fieldsConfig, dateFormatsConfig);
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

			if (dateFormats != null && !dateFormats.isEmpty()) {
				exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_NOT_PRECENT, dateFormatsConfig, fieldsConfig);
			}
		}
	}

	private void validateDBWriteAndInsertMode(RethinkDBSinkConfigBean configBean) {
		DBInsertMode dbInsertMode = configBean.getDBInsertMode();
		DBWriteMode dbWriteMode = configBean.getDBWriteMode();

		if (dbWriteMode != null) {
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
		} else {
			exception.add(ValidationConstant.Message.KEY_IS_INVALID, Sink.General.WRITE_MODE);
		}
	}

	private void setConfigBean(RethinkDBSinkConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, InvalidConfigException {
		configBean.setDBWriteMode(DBWriteMode.getInstance(ConfigUtil.getString(configs, Sink.General.WRITE_MODE)));
		configBean.setDBInsertMode(DBInsertMode.getInstance(ConfigUtil.getString(configs, Sink.General.INSERT_MODE)));
		configBean.setGenerateId(ConfigUtil.getBoolean(configs, Sink.General.GENERATE_ID, exception, true));
		configBean.setSoftDurability(ConfigUtil.getBoolean(configs, RethinkSink.SOFT_DURABILITY, exception, false));

		setFileds(configBean, configs);
		setRetryAndBatchConfigs(configBean, configs);
		setConstants(configBean, configs);
	}

	private void setConstants(RethinkDBSinkConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		configBean.setInsertConstantsConfigBean(getConstantsConfigBean(configs, RethinkSink.INSERT_CONSTANT_FIELDS, RethinkSink.INSERT_CONSTANT_TYPES,
		        RethinkSink.INSERT_CONSTANT_VALUES, RethinkSink.INSERT_LITERAL_CONSTANT_FIELDS, RethinkSink.INSERT_LITERAL_CONSTANT_TYPES,
		        RethinkSink.INSERT_LITERAL_CONSTANT_VALUES, RethinkSink.INSERT_LITERAL_CONSTANT_DATE_FORMATS));

		configBean.setUpdateConstantsConfigBean(getConstantsConfigBean(configs, RethinkSink.UPDATE_CONSTANT_FIELDS, RethinkSink.UPDATE_CONSTANT_TYPES,
		        RethinkSink.UPDATE_CONSTANT_VALUES, RethinkSink.UPDATE_LITERAL_CONSTANT_FIELDS, RethinkSink.UPDATE_LITERAL_CONSTANT_TYPES,
		        RethinkSink.UPDATE_LITERAL_CONSTANT_VALUES, RethinkSink.UPDATE_LITERAL_CONSTANT_DATE_FORMATS));
	}

	private NoSQLConstantsConfigBean getConstantsConfigBean(LinkedHashMap<String, String> configs, String fieldsConfig, String typesConfig, String valuesConfig,
	        String literalFieldsConfig, String literalTypesConfig, String literalValuesConfig, String literalDateFormatsConfig)
	        throws ImproperValidationException {

		NoSQLConstantsConfigBean constantsConfigBean = new NoSQLConstantsConfigBean();

		constantsConfigBean.setFields(ConfigUtil.getArrayList(configs, fieldsConfig, exception));
		constantsConfigBean.setTypes(ConfigUtil.getArrayListOfClass(configs, typesConfig, exception));
		constantsConfigBean.setValues(ConfigUtil.getArrayList(configs, valuesConfig, exception));

		constantsConfigBean.setLiteralFields(ConfigUtil.getArrayList(configs, literalFieldsConfig, exception));
		constantsConfigBean.setLiteralTypes(ConfigUtil.getArrayListOfClass(configs, literalTypesConfig, exception));
		constantsConfigBean.setLiteralValues(ConfigUtil.getArrayList(configs, literalValuesConfig, exception));
		constantsConfigBean.setDateFormats(ConfigUtil.getArrayList(configs, literalDateFormatsConfig, exception));

		return constantsConfigBean;
	}

	private void setRetryAndBatchConfigs(RethinkDBSinkConfigBean configBean, LinkedHashMap<String, String> configs) {
		configBean.setRethinkInitRetryConfigBean(ConfigUtil.getInteger(configs, Sink.General.INIT_MAX_RETRY_COUNT, exception, General.DEFAULT_MAX_RETRY_COUNT),
		        ConfigUtil.getLong(configs, Sink.General.INIT_RETRY_DELAY, exception, General.DEFAULT_RETRY_DELAY));

		configBean.setRethinkDestroyRetryConfigBean(
		        ConfigUtil.getInteger(configs, Sink.General.DESTROY_MAX_RETRY_COUNT, exception, General.DEFAULT_MAX_RETRY_COUNT),
		        ConfigUtil.getLong(configs, Sink.General.DESTROY_RETRY_DELAY, exception, General.DEFAULT_RETRY_DELAY));

		configBean.setBatchType(BatchType.getInstance(ConfigUtil.getString(configs, Sink.General.BATCH_TYPE)));
		configBean.setBatchSize(ConfigUtil.getLong(configs, Sink.General.BATCH_SIZE, exception, 0L));
	}

	private void setFileds(RethinkDBSinkConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		configBean.setKeyFields(ConfigUtil.getArrayList(configs, Sink.General.KEY_FIELDS, exception));
		configBean.setKeyColumns(ConfigUtil.getArrayList(configs, Sink.General.KEY_COLUMNS, exception));

		configBean.setFieldsDifferToColumns(ConfigUtil.getArrayList(configs, Sink.General.FIELD_NAMES_DIFFER_TO_COLUMNS, exception));
		configBean.setColumnsDifferToFields(ConfigUtil.getArrayList(configs, Sink.General.COLUMN_NAMES_DIFFER_TO_FIELDS, exception));

		configBean.setMetaDataFields(ConfigUtil.getArrayList(configs, Sink.General.META_DATA_FIELDS, exception));
	}

	private void setConnection(RethinkDBSinkConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		ArrayList<String> host = ConfigUtil.getArrayList(configs, SQLNoSQL.HOST, exception);
		ArrayList<Integer> portNumber = ConfigUtil.getArrayListOfIntegers(configs, SQLNoSQL.PORT_NUMBER, exception);
		String dbName = ConfigUtil.getString(configs, SQLNoSQL.DB_NAME);
		String userName = ConfigUtil.getString(configs, SQLNoSQL.USER_NAME);
		String password = ConfigUtil.getString(configs, SQLNoSQL.PASSWORD);
		Long timeout = ConfigUtil.getLong(configs, SQLNoSQL.TIMEOUT, exception, RethinkDefault.TIMEOUT);

		configBean.setConnection(host, portNumber, dbName, userName, password, timeout);

		validateConnection(configBean.getConnection());
	}

	private void validateConnection(RethinkDBConnection rethinkDBConnection) {
		if (rethinkDBConnection.getHost() != null && !rethinkDBConnection.getHost().isEmpty() && rethinkDBConnection.getPortNumber() != null
		        && !rethinkDBConnection.getPortNumber().isEmpty() && rethinkDBConnection.getHost().size() != rethinkDBConnection.getPortNumber().size()) {
			exception.add(Message.SIZE_SHOULD_MATCH, SQLNoSQL.HOST, SQLNoSQL.PORT_NUMBER);
		}
	}

	@Override
	public ConfigBean validateAndSetErrorHandler(LinkedHashMap<String, String> configs) throws ImproperValidationException, InvalidConfigException {
		RethinkDBSinkConfigBean sinkConfigBean = new RethinkDBSinkConfigBean();

		setErrorHandlerStore(sinkConfigBean, configs);
		validateErrorHandlerStore(sinkConfigBean, configs);
		sinkConfigBean.setName(ConfigUtil.getString(configs, General.NAME));
		sinkConfigBean.setExternalDataPrefix(ELTConstants.EXTERNAL_DATA);

		return sinkConfigBean;
	}

	private void setErrorHandlerStore(RethinkDBSinkConfigBean sinkConfigBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		setConnection(sinkConfigBean, configs);
		sinkConfigBean.setTableName(ConfigUtil.getString(configs, SQLNoSQL.TABLE));
		sinkConfigBean.setDBWriteMode(DBWriteMode.APPEND);
		sinkConfigBean.setDBInsertMode(DBInsertMode.INSERT);

		sinkConfigBean.setConstantsConfigBean(getConstantsConfigBean(configs, RethinkSink.CONSTANT_FIELDS, RethinkSink.CONSTANT_TYPES,
		        RethinkSink.CONSTANT_VALUES, RethinkSink.LITERAL_CONSTANT_FIELDS, RethinkSink.LITERAL_CONSTANT_TYPES, RethinkSink.LITERAL_CONSTANT_VALUES,
		        RethinkSink.LITERAL_CONSTANT_DATE_FORMATS));
	}

	private RethinkDBSinkConfigBean validateErrorHandlerStore(RethinkDBSinkConfigBean sinkConfigBean, LinkedHashMap<String, String> configs)
	        throws InvalidConfigException {
		validateMandatoryFields(sinkConfigBean);
		validateDBWriteAndInsertMode(sinkConfigBean);
		validateDBConstants(sinkConfigBean.getConstantsConfigBean(), sinkConfigBean.getDBInsertMode(), RethinkSink.CONSTANT_FIELDS, RethinkSink.CONSTANT_TYPES,
		        RethinkSink.CONSTANT_VALUES, RethinkSink.LITERAL_CONSTANT_FIELDS, RethinkSink.LITERAL_CONSTANT_TYPES, RethinkSink.LITERAL_CONSTANT_VALUES,
		        RethinkSink.LITERAL_CONSTANT_DATE_FORMATS, DBInsertMode.UPSERT, false);

		if (exception.getNumberOfExceptions() > 0) {
			throw exception;
		} else {
			return sinkConfigBean;
		}
	}

	private void validateMandatoryFields(RethinkDBSinkConfigBean sinkConfigBean) {
		if (sinkConfigBean.getConnection().getHost() == null || sinkConfigBean.getConnection().getHost().isEmpty()) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, SQLNoSQL.HOST);
		}

		if (sinkConfigBean.getTableName() == null || sinkConfigBean.getTableName().isEmpty()) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, SQLNoSQL.TABLE);
		}
	}

	@Override
	public void replaceWithComponentSpecific(ErrorHandlerSink errorHandlerSink, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, InvalidConfigException {
		RethinkDBSinkConfigBean sinkConfigBean = (RethinkDBSinkConfigBean) errorHandlerSink;

		if (!ConfigUtil.isAllEmpty(configs, SQLNoSQL.HOST, SQLNoSQL.PORT_NUMBER, SQLNoSQL.DB_NAME, SQLNoSQL.USER_NAME, SQLNoSQL.PASSWORD, SQLNoSQL.TIMEOUT)) {
			ArrayList<String> host = configs.containsKey(SQLNoSQL.HOST) ? ConfigUtil.getArrayList(configs, SQLNoSQL.HOST, exception)
			        : sinkConfigBean.getConnection().getHost();
			ArrayList<Integer> portNumber = configs.containsKey(SQLNoSQL.PORT_NUMBER)
			        ? ConfigUtil.getArrayListOfIntegers(configs, SQLNoSQL.PORT_NUMBER, exception)
			        : sinkConfigBean.getConnection().getPortNumber();
			String dbName = configs.containsKey(SQLNoSQL.DB_NAME) ? ConfigUtil.getString(configs, SQLNoSQL.DB_NAME)
			        : sinkConfigBean.getConnection().getDBName();
			String userName = configs.containsKey(SQLNoSQL.USER_NAME) ? ConfigUtil.getString(configs, SQLNoSQL.USER_NAME)
			        : sinkConfigBean.getConnection().getUserName();
			String password = configs.containsKey(SQLNoSQL.PASSWORD) ? ConfigUtil.getString(configs, SQLNoSQL.PASSWORD)
			        : sinkConfigBean.getConnection().getPassword();
			Long timeout = configs.containsKey(SQLNoSQL.TIMEOUT) ? ConfigUtil.getLong(configs, SQLNoSQL.TIMEOUT, exception, RethinkDefault.TIMEOUT)
			        : sinkConfigBean.getConnection().getTimeout();
			sinkConfigBean.setConnection(host, portNumber, dbName, userName, password, timeout);
		}

		if (!ConfigUtil.isAllEmpty(configs, RethinkSink.CONSTANT_FIELDS, RethinkSink.CONSTANT_TYPES, RethinkSink.CONSTANT_VALUES,
		        RethinkSink.LITERAL_CONSTANT_FIELDS, RethinkSink.LITERAL_CONSTANT_TYPES, RethinkSink.LITERAL_CONSTANT_VALUES,
		        RethinkSink.LITERAL_CONSTANT_DATE_FORMATS)) {
			ArrayList<String> constantsFields = configs.containsKey(RethinkSink.CONSTANT_FIELDS)
			        ? ConfigUtil.getArrayList(configs, RethinkSink.CONSTANT_FIELDS, exception)
			        : sinkConfigBean.getConstantsConfigBean().getFields();
			ArrayList<Class<?>> constantsTypes = configs.containsKey(RethinkSink.CONSTANT_TYPES)
			        ? ConfigUtil.getArrayListOfClass(configs, RethinkSink.CONSTANT_TYPES, exception)
			        : sinkConfigBean.getConstantsConfigBean().getTypes();
			ArrayList<String> constantsValues = configs.containsKey(RethinkSink.CONSTANT_VALUES)
			        ? ConfigUtil.getArrayList(configs, RethinkSink.CONSTANT_VALUES, exception)
			        : sinkConfigBean.getConstantsConfigBean().getValues();

			ArrayList<String> literalFields = configs.containsKey(RethinkSink.LITERAL_CONSTANT_FIELDS)
			        ? ConfigUtil.getArrayList(configs, RethinkSink.LITERAL_CONSTANT_FIELDS, exception)
			        : sinkConfigBean.getConstantsConfigBean().getLiteralFields();
			ArrayList<Class<?>> literalTypes = configs.containsKey(RethinkSink.LITERAL_CONSTANT_TYPES)
			        ? ConfigUtil.getArrayListOfClass(configs, RethinkSink.LITERAL_CONSTANT_TYPES, exception)
			        : sinkConfigBean.getConstantsConfigBean().getLiteralTypes();
			ArrayList<String> literalValues = configs.containsKey(RethinkSink.LITERAL_CONSTANT_VALUES)
			        ? ConfigUtil.getArrayList(configs, RethinkSink.LITERAL_CONSTANT_VALUES, exception)
			        : sinkConfigBean.getConstantsConfigBean().getLiteralValues();
			ArrayList<String> literalDateFormats = configs.containsKey(RethinkSink.LITERAL_CONSTANT_DATE_FORMATS)
			        ? ConfigUtil.getArrayList(configs, RethinkSink.LITERAL_CONSTANT_DATE_FORMATS, exception)
			        : sinkConfigBean.getConstantsConfigBean().getDateFormats();

			NoSQLConstantsConfigBean rethinkDBConstantsConfigBean = new NoSQLConstantsConfigBean(constantsFields, constantsTypes, constantsValues,
			        literalFields, literalTypes, literalValues, literalDateFormats);
			sinkConfigBean.setConstantsConfigBean(rethinkDBConstantsConfigBean);
		}

		sinkConfigBean.setTableName(configs.containsKey(SQLNoSQL.TABLE) ? ConfigUtil.getString(configs, SQLNoSQL.TABLE) : sinkConfigBean.getTableName());
		sinkConfigBean.setDBInsertMode(configs.containsKey(Sink.General.INSERT_MODE)
		        ? DBInsertMode.getInstance(ConfigUtil.getString(configs, Sink.General.INSERT_MODE), DBInsertMode.INSERT)
		        : sinkConfigBean.getDBInsertMode());
		sinkConfigBean.setDBWriteMode(configs.containsKey(Sink.General.WRITE_MODE)
		        ? DBWriteMode.getInstance(ConfigUtil.getString(configs, Sink.General.WRITE_MODE), DBWriteMode.APPEND)
		        : sinkConfigBean.getDBWriteMode());

		validateConnection(sinkConfigBean.getConnection());
		validateErrorHandlerStore(sinkConfigBean, configs);
	}
}

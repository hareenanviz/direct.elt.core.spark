package com.anvizent.elt.core.spark.sink.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.commons.constants.Constants.ELTConstants;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.connection.ArangoDBConnection;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.BatchType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.ArangoDBDefault;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink.ArangoDBSink;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink.RethinkSink;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.constant.DBWriteMode;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.sink.config.bean.ArangoDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.NoSQLConstantsConfigBean;
import com.anvizent.elt.core.spark.validator.ErrorHandlerValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class ArangoDBSinkValidator extends Validator implements ErrorHandlerValidator {

	private static final long serialVersionUID = 1L;

	public ArangoDBSinkValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {
		ArangoDBSinkConfigBean arangoDBSinkConfigBean = new ArangoDBSinkConfigBean();

		validateMandatoryFields(configs, SQLNoSQL.HOST, SQLNoSQL.PORT_NUMBER, SQLNoSQL.TABLE);

		validateAndSetConfigBean(arangoDBSinkConfigBean, configs, configAndMappingConfigBeans);

		return arangoDBSinkConfigBean;
	}

	private void validateAndSetConfigBean(ArangoDBSinkConfigBean arangoDBSinkConfigBean, LinkedHashMap<String, String> configs,
	        ConfigAndMappingConfigBeans configAndMappingConfigBeans) throws ImproperValidationException {
		setConnection(arangoDBSinkConfigBean, configs);
		setConfigBean(arangoDBSinkConfigBean, configs);

		if (arangoDBSinkConfigBean.getDBInsertMode() != null) {
			if (!arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT)
			        && (arangoDBSinkConfigBean.getKeyFields() == null || arangoDBSinkConfigBean.getKeyFields().isEmpty())) {
				exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, Sink.General.KEY_FIELDS);
			} else if (arangoDBSinkConfigBean.getKeyFields() != null && !arangoDBSinkConfigBean.getKeyFields().isEmpty()
			        && arangoDBSinkConfigBean.getKeyColumns() != null && !arangoDBSinkConfigBean.getKeyColumns().isEmpty()
			        && arangoDBSinkConfigBean.getKeyFields().size() != arangoDBSinkConfigBean.getKeyColumns().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Sink.General.KEY_FIELDS, Sink.General.KEY_COLUMNS);
			} else if (arangoDBSinkConfigBean.getKeyFields() != null && !arangoDBSinkConfigBean.getKeyFields().isEmpty()
			        && arangoDBSinkConfigBean.getKeyColumns() != null && !arangoDBSinkConfigBean.getKeyColumns().isEmpty()) {
				if (arangoDBSinkConfigBean.getMetaDataFields() != null && !arangoDBSinkConfigBean.getMetaDataFields().isEmpty()
				        && (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)
				                || arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT))) {

					ArrayList<String> keyFields = arangoDBSinkConfigBean.getKeyColumns() == null || arangoDBSinkConfigBean.getKeyColumns().isEmpty()
					        ? arangoDBSinkConfigBean.getKeyFields()
					        : arangoDBSinkConfigBean.getKeyColumns();
					for (String metaDataField : arangoDBSinkConfigBean.getMetaDataFields()) {
						if (keyFields.contains(metaDataField)) {
							exception.add(ValidationConstant.Message.KEY_IS_PRESENT_IN_OTHER, Sink.General.META_DATA_FIELDS, Sink.General.KEY_FIELDS);
							break;
						}
					}
				}
			}

			if (arangoDBSinkConfigBean.getColumnsDifferToFields() != null && !arangoDBSinkConfigBean.getColumnsDifferToFields().isEmpty()) {
				if (arangoDBSinkConfigBean.getFieldsDifferToColumns() == null || arangoDBSinkConfigBean.getFieldsDifferToColumns().isEmpty()) {
					exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, Sink.General.FIELD_NAMES_DIFFER_TO_COLUMNS);
				} else if (arangoDBSinkConfigBean.getColumnsDifferToFields().size() != arangoDBSinkConfigBean.getFieldsDifferToColumns().size()) {
					exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Sink.General.FIELD_NAMES_DIFFER_TO_COLUMNS,
					        Sink.General.COLUMN_NAMES_DIFFER_TO_FIELDS);
				}
			}

			arangoDBSinkConfigBean(arangoDBSinkConfigBean, configs);
			validateDBConstants(arangoDBSinkConfigBean);
			validateBatchTypeAndSize(arangoDBSinkConfigBean, configAndMappingConfigBeans);
		} else {
			exception.add(ValidationConstant.Message.KEY_IS_INVALID, Sink.General.INSERT_MODE);
		}
	}

	private void validateBatchTypeAndSize(ArangoDBSinkConfigBean arangoDBSinkConfigBean, ConfigAndMappingConfigBeans configAndMappingConfigBeans) {
		if (arangoDBSinkConfigBean.getBatchType() == null) {
			exception.add(ValidationConstant.Message.KEY_IS_INVALID, Sink.General.BATCH_TYPE);
		} else if (arangoDBSinkConfigBean.getBatchType().equals(BatchType.ALL) || arangoDBSinkConfigBean.getBatchType().equals(BatchType.NONE)) {
			if (arangoDBSinkConfigBean.getBatchSize() != 0) {
				exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_PRECENT, Sink.General.BATCH_SIZE,
				        Sink.General.BATCH_SIZE + ": '" + arangoDBSinkConfigBean.getBatchType() + "'");
			}
		} else {
			if (arangoDBSinkConfigBean.getBatchSize() <= 0) {
				exception.add(ValidationConstant.Message.INVALID_VALUE_FOR, arangoDBSinkConfigBean.getBatchSize(), Sink.General.BATCH_SIZE);
			}
		}
	}

	private void validateDBConstants(ArangoDBSinkConfigBean arangoDBSinkConfigBean) {
		validateDBConstants(arangoDBSinkConfigBean.getConstantsConfigBean(), arangoDBSinkConfigBean.getDBInsertMode(), ArangoDBSink.CONSTANT_FIELDS,
		        ArangoDBSink.CONSTANT_TYPES, ArangoDBSink.CONSTANT_VALUES, ArangoDBSink.LITERAL_CONSTANT_FIELDS, ArangoDBSink.LITERAL_CONSTANT_TYPES,
		        ArangoDBSink.LITERAL_CONSTANT_VALUES, ArangoDBSink.LITERAL_CONSTANT_DATE_FORMATS, DBInsertMode.UPSERT, false);

		validateDBConstants(arangoDBSinkConfigBean.getInsertConstantsConfigBean(), arangoDBSinkConfigBean.getDBInsertMode(),
		        ArangoDBSink.INSERT_CONSTANT_FIELDS, ArangoDBSink.INSERT_CONSTANT_TYPES, ArangoDBSink.INSERT_CONSTANT_VALUES,
		        ArangoDBSink.INSERT_LITERAL_CONSTANT_FIELDS, ArangoDBSink.INSERT_LITERAL_CONSTANT_TYPES, ArangoDBSink.INSERT_LITERAL_CONSTANT_VALUES,
		        ArangoDBSink.INSERT_LITERAL_CONSTANT_DATE_FORMATS, DBInsertMode.UPSERT, true);

		validateDBConstants(arangoDBSinkConfigBean.getUpdateConstantsConfigBean(), arangoDBSinkConfigBean.getDBInsertMode(),
		        ArangoDBSink.UPDATE_CONSTANT_FIELDS, ArangoDBSink.UPDATE_CONSTANT_TYPES, ArangoDBSink.UPDATE_CONSTANT_VALUES,
		        ArangoDBSink.UPDATE_LITERAL_CONSTANT_FIELDS, ArangoDBSink.UPDATE_LITERAL_CONSTANT_TYPES, ArangoDBSink.UPDATE_LITERAL_CONSTANT_VALUES,
		        ArangoDBSink.UPDATE_LITERAL_CONSTANT_DATE_FORMATS, DBInsertMode.UPSERT, true);
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

	private void arangoDBSinkConfigBean(ArangoDBSinkConfigBean arangoDBSinkConfigBean, LinkedHashMap<String, String> configs) {
		DBInsertMode dbInsertMode = arangoDBSinkConfigBean.getDBInsertMode();
		DBWriteMode dbWriteMode = arangoDBSinkConfigBean.getDBWriteMode();

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

	private void setConfigBean(ArangoDBSinkConfigBean arangoDBSinkConfigBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		arangoDBSinkConfigBean.setTableName(ConfigUtil.getString(configs, SQLNoSQL.TABLE));
		arangoDBSinkConfigBean.setDBWriteMode(DBWriteMode.getInstance(ConfigUtil.getString(configs, Sink.General.WRITE_MODE)));
		arangoDBSinkConfigBean.setDBInsertMode(DBInsertMode.getInstance(ConfigUtil.getString(configs, Sink.General.INSERT_MODE)));
		arangoDBSinkConfigBean.setGenerateId(ConfigUtil.getBoolean(configs, Sink.General.GENERATE_ID, exception, true));
		arangoDBSinkConfigBean.setWaitForSync(ConfigUtil.getBoolean(configs, Sink.ArangoDBSink.WAIT_FOR_SYNC, exception, true));

		setFileds(arangoDBSinkConfigBean, configs);
		setRetryAndBatchConfigs(arangoDBSinkConfigBean, configs);
		setConstants(arangoDBSinkConfigBean, configs);
	}

	private void setConstants(ArangoDBSinkConfigBean arangoDBSinkConfigBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		arangoDBSinkConfigBean.setConstantsConfigBean(getConstantsConfigBean(configs, ArangoDBSink.CONSTANT_FIELDS, ArangoDBSink.CONSTANT_TYPES,
		        ArangoDBSink.CONSTANT_VALUES, ArangoDBSink.LITERAL_CONSTANT_FIELDS, ArangoDBSink.LITERAL_CONSTANT_TYPES, ArangoDBSink.LITERAL_CONSTANT_VALUES,
		        ArangoDBSink.LITERAL_CONSTANT_DATE_FORMATS));

		arangoDBSinkConfigBean.setInsertConstantsConfigBean(getConstantsConfigBean(configs, ArangoDBSink.INSERT_CONSTANT_FIELDS,
		        ArangoDBSink.INSERT_CONSTANT_TYPES, ArangoDBSink.INSERT_CONSTANT_VALUES, ArangoDBSink.INSERT_LITERAL_CONSTANT_FIELDS,
		        ArangoDBSink.INSERT_LITERAL_CONSTANT_TYPES, ArangoDBSink.INSERT_LITERAL_CONSTANT_VALUES, ArangoDBSink.INSERT_LITERAL_CONSTANT_DATE_FORMATS));

		arangoDBSinkConfigBean.setUpdateConstantsConfigBean(getConstantsConfigBean(configs, ArangoDBSink.UPDATE_CONSTANT_FIELDS,
		        ArangoDBSink.UPDATE_CONSTANT_TYPES, ArangoDBSink.UPDATE_CONSTANT_VALUES, ArangoDBSink.UPDATE_LITERAL_CONSTANT_FIELDS,
		        ArangoDBSink.UPDATE_LITERAL_CONSTANT_TYPES, ArangoDBSink.UPDATE_LITERAL_CONSTANT_VALUES, ArangoDBSink.UPDATE_LITERAL_CONSTANT_DATE_FORMATS));
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

	private void setRetryAndBatchConfigs(ArangoDBSinkConfigBean arangoDBSinkConfigBean, LinkedHashMap<String, String> configs) {
		arangoDBSinkConfigBean.setInitRetryConfigBean(
		        ConfigUtil.getInteger(configs, Sink.General.INIT_MAX_RETRY_COUNT, exception, General.DEFAULT_MAX_RETRY_COUNT),
		        ConfigUtil.getLong(configs, Sink.General.INIT_RETRY_DELAY, exception, General.DEFAULT_RETRY_DELAY));

		arangoDBSinkConfigBean.setDestroyRetryConfigBean(
		        ConfigUtil.getInteger(configs, Sink.General.DESTROY_MAX_RETRY_COUNT, exception, General.DEFAULT_MAX_RETRY_COUNT),
		        ConfigUtil.getLong(configs, Sink.General.DESTROY_RETRY_DELAY, exception, General.DEFAULT_RETRY_DELAY));

		arangoDBSinkConfigBean.setBatchType(BatchType.getInstance(ConfigUtil.getString(configs, Sink.General.BATCH_TYPE)));
		arangoDBSinkConfigBean.setBatchSize(ConfigUtil.getLong(configs, Sink.General.BATCH_SIZE, exception, 0L));
	}

	private void setFileds(ArangoDBSinkConfigBean arangoDBSinkConfigBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		arangoDBSinkConfigBean.setKeyFields(ConfigUtil.getArrayList(configs, Sink.General.KEY_FIELDS, exception));
		arangoDBSinkConfigBean.setKeyColumns(ConfigUtil.getArrayList(configs, Sink.General.KEY_COLUMNS, exception));

		arangoDBSinkConfigBean.setFieldsDifferToColumns(ConfigUtil.getArrayList(configs, Sink.General.FIELD_NAMES_DIFFER_TO_COLUMNS, exception));
		arangoDBSinkConfigBean.setColumnsDifferToFields(ConfigUtil.getArrayList(configs, Sink.General.COLUMN_NAMES_DIFFER_TO_FIELDS, exception));

		arangoDBSinkConfigBean.setMetaDataFields(ConfigUtil.getArrayList(configs, Sink.General.META_DATA_FIELDS, exception));
	}

	private void setConnection(ArangoDBSinkConfigBean arangoDBSinkConfigBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		ArrayList<String> host = ConfigUtil.getArrayList(configs, SQLNoSQL.HOST, exception);
		ArrayList<Integer> portNumber = ConfigUtil.getArrayListOfIntegers(configs, SQLNoSQL.PORT_NUMBER, exception);
		String dbName = ConfigUtil.getString(configs, SQLNoSQL.DB_NAME, ArangoDBDefault.DB_NAME);
		String userName = ConfigUtil.getString(configs, SQLNoSQL.USER_NAME);
		String password = ConfigUtil.getString(configs, SQLNoSQL.PASSWORD);
		Integer timeout = ConfigUtil.getInteger(configs, SQLNoSQL.TIMEOUT, exception, ArangoDBDefault.TIMEOUT);

		arangoDBSinkConfigBean.setConnection(host, portNumber, dbName, userName, password, timeout);

		validateConnection(arangoDBSinkConfigBean.getConnection());
	}

	private void validateConnection(ArangoDBConnection arangoDBConnection) {
		if (arangoDBConnection.getHost() != null && !arangoDBConnection.getHost().isEmpty() && arangoDBConnection.getPortNumber() != null
		        && !arangoDBConnection.getPortNumber().isEmpty() && arangoDBConnection.getHost().size() != arangoDBConnection.getPortNumber().size()) {
			exception.add(Message.SIZE_SHOULD_MATCH, SQLNoSQL.HOST, SQLNoSQL.PORT_NUMBER);
		}
	}

	@Override
	public ConfigBean validateAndSetErrorHandler(LinkedHashMap<String, String> configs) throws ImproperValidationException, InvalidConfigException {
		ArangoDBSinkConfigBean sinkConfigBean = new ArangoDBSinkConfigBean();

		setErrorHandlerStore(sinkConfigBean, configs);
		validateErrorHandlerStore(sinkConfigBean, configs);
		sinkConfigBean.setName(ConfigUtil.getString(configs, General.NAME));
		sinkConfigBean.setExternalDataPrefix(ELTConstants.EXTERNAL_DATA);

		return sinkConfigBean;
	}

	private void setErrorHandlerStore(ArangoDBSinkConfigBean sinkConfigBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		setConnection(sinkConfigBean, configs);
		sinkConfigBean.setTableName(ConfigUtil.getString(configs, SQLNoSQL.TABLE));
		sinkConfigBean.setDBWriteMode(DBWriteMode.APPEND);
		sinkConfigBean.setDBInsertMode(DBInsertMode.INSERT);

		sinkConfigBean.setConstantsConfigBean(getConstantsConfigBean(configs, RethinkSink.CONSTANT_FIELDS, RethinkSink.CONSTANT_TYPES,
		        RethinkSink.CONSTANT_VALUES, RethinkSink.LITERAL_CONSTANT_FIELDS, RethinkSink.LITERAL_CONSTANT_TYPES, RethinkSink.LITERAL_CONSTANT_VALUES,
		        RethinkSink.LITERAL_CONSTANT_DATE_FORMATS));
	}

	private void validateDBWriteAndInsertMode(ArangoDBSinkConfigBean configBean) {
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

	private ArangoDBSinkConfigBean validateErrorHandlerStore(ArangoDBSinkConfigBean sinkConfigBean, LinkedHashMap<String, String> configs)
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

	private void validateMandatoryFields(ArangoDBSinkConfigBean sinkConfigBean) {
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
		ArangoDBSinkConfigBean sinkConfigBean = (ArangoDBSinkConfigBean) errorHandlerSink;

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
			Integer timeout = configs.containsKey(SQLNoSQL.TIMEOUT) ? ConfigUtil.getInteger(configs, SQLNoSQL.TIMEOUT, exception, ArangoDBDefault.TIMEOUT)
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

package com.anvizent.elt.core.spark.source.validator;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Source;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Source.SourceSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.SparkConstants.Options;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.source.config.bean.SourceSQLConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class SourceSQLValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public SourceSQLValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException {

		validateMandatoryFields(configs, SQLNoSQL.JDBC_URL, SQLNoSQL.DRIVER, SQLNoSQL.USER_NAME, SQLNoSQL.PASSWORD, SQLNoSQL.IS_QUERY,
		        SourceSQL.TABLE_NAME_OR_QUERY);

		SourceSQLConfigBean configBean = new SourceSQLConfigBean();

		setConfigBean(configBean, configs);

		getOptions(configBean, configs);

		validatePartitions(configBean);

		return configBean;
	}

	private void setConfigBean(SourceSQLConfigBean configBean, LinkedHashMap<String, String> configs) {
		String jdbcURL = ConfigUtil.getString(configs, SQLNoSQL.JDBC_URL);
		String driver = ConfigUtil.getString(configs, SQLNoSQL.DRIVER);
		String userName = ConfigUtil.getString(configs, SQLNoSQL.USER_NAME);
		String password = ConfigUtil.getString(configs, SQLNoSQL.PASSWORD);

		configBean.setConnection(jdbcURL, driver, userName, password);
		configBean.setTableName(ConfigUtil.getString(configs, SourceSQL.TABLE_NAME_OR_QUERY));
		configBean.setQuery(ConfigUtil.getBoolean(configs, SQLNoSQL.IS_QUERY, exception));
		configBean.setTableName(configBean.isQuery());
		configBean.setQueryAlias(ConfigUtil.getString(configs, SQLNoSQL.QUERY_ALIAS));
		configBean.setNumberOfPartitions(ConfigUtil.getInteger(configs, Source.General.NUMBER_OF_PARTITIONS, exception));
		configBean.setPartitionsSize(ConfigUtil.getLong(configs, Source.General.PARTITIONS_SIZE, exception));
		configBean.setQueryContainsOffset(ConfigUtil.getBoolean(configs, SourceSQL.QUERY_CONTAINS_OFFSET, exception));
	}

	private void getOptions(SourceSQLConfigBean configBean, LinkedHashMap<String, String> configs) {
		Map<String, String> options = new LinkedHashMap<String, String>();

		boolean isAllNotEmpty = ConfigUtil.isAllNotEmpty(configs, SourceSQL.PARTITION_COLUMN, SourceSQL.LOWER_BOUND, SourceSQL.UPPER_BOUND,
		        SourceSQL.NUMBER_OF_PARTITIONS);

		if (isAllNotEmpty) {
			options.put(Options.PARTITION_COLUMN, configs.get(SourceSQL.PARTITION_COLUMN));
			options.put(Options.LOWER_BOUND, configs.get(SourceSQL.LOWER_BOUND));
			options.put(Options.UPPER_BOUND, configs.get(SourceSQL.UPPER_BOUND));
			options.put(Options.NUMBER_OF_PARTITIONS, configs.get(SourceSQL.NUMBER_OF_PARTITIONS));
		} else {
			boolean isAllEmpty = ConfigUtil.isAllEmpty(configs, SourceSQL.PARTITION_COLUMN, SourceSQL.LOWER_BOUND, SourceSQL.UPPER_BOUND,
			        SourceSQL.NUMBER_OF_PARTITIONS);
			if (!isAllEmpty) {
				exception.add(ValidationConstant.Message.EITHER_FOURE_ARE_MANDATORY_OR_NONE, SourceSQL.PARTITION_COLUMN, SourceSQL.LOWER_BOUND,
				        SourceSQL.UPPER_BOUND, SourceSQL.NUMBER_OF_PARTITIONS);
			}
		}

		configBean.setOptions(options);
	}

	private void validatePartitions(SourceSQLConfigBean configBean) {
		if (configBean.isQuery() && (configBean.getNumberOfPartitions() != null || configBean.getPartitionsSize() != null)
		        && StringUtils.isBlank(configBean.getQueryAlias())) {
			exception.add(ValidationConstant.Message.IS_MANDATORY_FOR_VALUE_1_AND_2_OR_3_IS_PRESENT, SQLNoSQL.QUERY_ALIAS, SQLNoSQL.IS_QUERY,
			        configBean.isQuery(), Source.General.NUMBER_OF_PARTITIONS, Source.General.PARTITIONS_SIZE);
		}

		if (configBean.getNumberOfPartitions() != null && configBean.getPartitionsSize() != null) {
			exception.add(ValidationConstant.Message.EITHER_BOTH_ARE_MANDATORY_OR_NONE, configBean.isQuery(), Source.General.NUMBER_OF_PARTITIONS,
			        Source.General.PARTITIONS_SIZE);
		}
	}

}

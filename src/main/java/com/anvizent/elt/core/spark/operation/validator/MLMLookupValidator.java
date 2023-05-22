package com.anvizent.elt.core.spark.operation.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.MLM;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.MLMLookupConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class MLMLookupValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public MLMLookupValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException {
		validateMandatoryFields(configs, MLM.QUERY_URL, MLM.INITIATE_URL, MLM.ADD_TO_TASK_KILL_LIST_URL, MLM.USER_ID, MLM.CLIENT_ID, MLM.PRIVATE_KEY, MLM.IV,
		        MLM.DATA_SOURCE, MLM.KAFKA_BOOTSTRAP_SERVERS, MLM.KAFKA_TOPIC, MLM.TASK_DURATION);

		MLMLookupConfigBean configBean = new MLMLookupConfigBean();
		setConfigBean(configBean, configs);

		if (!configBean.isIncremental() && configBean.getKeyFields() != null && configBean.getKeyFields().size() > 0) {
			exception.add(ExceptionMessage.INVALID_INVALID_TO_SPECIFY_WHEN_VALUE_IS, MLM.KEY_FIELDS, MLM.IS_INCREMENTAL, configBean.isIncremental());
		}

		return configBean;
	}

	private void setConfigBean(MLMLookupConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, InvalidConfigException {
		configBean.setQueryURL(ConfigUtil.getString(configs, MLM.QUERY_URL));
		configBean.setInitiateURL(ConfigUtil.getString(configs, MLM.INITIATE_URL));
		configBean.setAddToTaskKillListURL(ConfigUtil.getString(configs, MLM.ADD_TO_TASK_KILL_LIST_URL));
		configBean.setUserId(ConfigUtil.getString(configs, MLM.USER_ID));
		configBean.setClientId(ConfigUtil.getString(configs, MLM.CLIENT_ID));
		configBean.setPrivateKey(ConfigUtil.getString(configs, MLM.PRIVATE_KEY));
		configBean.setIv(ConfigUtil.getString(configs, MLM.IV));
		configBean.setTableName(ConfigUtil.getString(configs, MLM.DATA_SOURCE));
		configBean.setIncremental(ConfigUtil.getBoolean(configs, MLM.IS_INCREMENTAL, exception));
		configBean.setKafkaTopic(ConfigUtil.getString(configs, MLM.KAFKA_TOPIC));
		configBean.setKafkaBootstrapServers(ConfigUtil.getString(configs, MLM.KAFKA_BOOTSTRAP_SERVERS));
		configBean.setTaskCount(ConfigUtil.getLong(configs, MLM.TASK_COUNT, exception, MLM.TASK_COUNT_DEFAULT));
		configBean.setReplicas(ConfigUtil.getLong(configs, MLM.REPLICAS, exception, MLM.REPLICAS_DEFAULT));
		configBean.setTaskDuration(ConfigUtil.getString(configs, MLM.TASK_DURATION));
		configBean.setKeyFields(ConfigUtil.getArrayList(configs, MLM.KEY_FIELDS, exception));
		configBean.setDruidTimeFormat(ConfigUtil.getString(configs, MLM.DRUID_TIME_FORMAT, MLM.DEFAULT_DRUID_TIME_FORMAT));
	}
}

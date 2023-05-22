package com.anvizent.elt.core.spark.sink.validator;

import java.util.LinkedHashMap;
import java.util.Map;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Messaging;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.constant.KafkaSinkCompression;
import com.anvizent.elt.core.spark.constant.KafkaSinkWriteFormat;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.sink.config.bean.KafkaSinkConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class KafkaSinkValidator extends Validator {
	private static final long serialVersionUID = 1L;

	public KafkaSinkValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException {
		KafkaSinkConfigBean configBean = new KafkaSinkConfigBean();

		setConfigBean(configBean, configs);
		getOptions(configBean, configs);

		return configBean;
	}

	private void setConfigBean(KafkaSinkConfigBean configBean, LinkedHashMap<String, String> configs) {
		validateMandatoryFields(configs, Messaging.BOOTSTRAP_SERVERS);
		configBean.setBootstrapServers(ConfigUtil.getString(configs, Messaging.BOOTSTRAP_SERVERS));

		if (ConfigUtil.isAllEmpty(configs, Messaging.TOPIC, Messaging.TOPIC_FIELD)) {
			exception.add(Message.EITHER_OF_THE_KEY_IS_MANDATORY, Messaging.TOPIC, Messaging.TOPIC_FIELD);
		}

		configBean.setTopic(ConfigUtil.getString(configs, Messaging.TOPIC));
		configBean.setTopicField(ConfigUtil.getString(configs, Messaging.TOPIC_FIELD));
		configBean.setKeyField(ConfigUtil.getString(configs, Messaging.KEY_FIELD));
		configBean.setDateFormat(ConfigUtil.getString(configs, Messaging.DATE_FORMAT));
		configBean.setFormat(ConfigUtil.getKafkaSinkWriteFormat(configs, Messaging.FORMAT, exception, KafkaSinkWriteFormat.JSON));
	}

	private void getOptions(KafkaSinkConfigBean configBean, LinkedHashMap<String, String> configs) {
		LinkedHashMap<String, String> options = new LinkedHashMap<>();

		add(Messaging.BOOTSTRAP_SERVERS, configBean.getBootstrapServers(), options);
		add(Messaging.BUFFER_MEMORY, ConfigUtil.getLong(configs, Messaging.BUFFER_MEMORY, exception), options);
		add(Messaging.COMPRESSION_TYPE, ConfigUtil.getKafkaSinkCompression(configs, Messaging.COMPRESSION_TYPE, exception, null), options);
		add(Messaging.RETRIES, ConfigUtil.getInteger(configs, Messaging.RETRIES, exception, 3), options);
		add(Messaging.BATCH_SIZE, ConfigUtil.getInteger(configs, Messaging.BATCH_SIZE, exception), options);
		add(Messaging.CONNECTIONS_MAX_IDLE_MS, ConfigUtil.getLong(configs, Messaging.CONNECTIONS_MAX_IDLE_MS, exception), options);
		add(Messaging.LINGER_MS, ConfigUtil.getLong(configs, Messaging.LINGER_MS, exception), options);
		add(Messaging.MAX_BLOCK_MS, ConfigUtil.getLong(configs, Messaging.MAX_BLOCK_MS, exception), options);
		add(Messaging.REQUEST_TIMEOUT_MS, ConfigUtil.getLong(configs, Messaging.REQUEST_TIMEOUT_MS, exception), options);
		add(Messaging.RECONNECT_BACKOFF_MAX_MS, ConfigUtil.getLong(configs, Messaging.RECONNECT_BACKOFF_MAX_MS, exception), options);
		add(Messaging.RECONNECT_BACKOFF_MS, ConfigUtil.getLong(configs, Messaging.RECONNECT_BACKOFF_MS, exception), options);
		add(Messaging.RETRY_BACKOFF_MS, ConfigUtil.getLong(configs, Messaging.RETRY_BACKOFF_MS, exception), options);

		configBean.setConfig(options);
	}

	private void add(String key, Object value, Map<String, String> options) {
		if (value == null) {
			return;
		} else {
			if (value instanceof KafkaSinkCompression) {
				options.put(Messaging.PREFIX + key, ((KafkaSinkCompression) value).name());
			} else {
				options.put(Messaging.PREFIX + key, value.toString());
			}
		}
	}
}
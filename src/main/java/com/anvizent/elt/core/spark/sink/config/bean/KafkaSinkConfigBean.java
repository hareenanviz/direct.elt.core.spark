package com.anvizent.elt.core.spark.sink.config.bean;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.RetryMandatoryConfigBean;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.constant.KafkaSinkWriteFormat;

/**
 * @author Hareen Bejjanki
 *
 */
public class KafkaSinkConfigBean extends ConfigBean implements SinkConfigBean, RetryMandatoryConfigBean, ErrorHandlerSink {

	private static final long serialVersionUID = 1L;

	private String bootstrapServers;
	private String topic;
	private String topicField;
	private String keyField;
	private KafkaSinkWriteFormat format;
	private String dateFormat;
	private LinkedHashMap<String, String> config = new LinkedHashMap<>();

	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getTopicField() {
		return topicField;
	}

	public void setTopicField(String topicField) {
		this.topicField = topicField;
	}

	public String getKeyField() {
		return keyField;
	}

	public void setKeyField(String keyField) {
		this.keyField = keyField;
	}

	public KafkaSinkWriteFormat getFormat() {
		return format;
	}

	public void setFormat(KafkaSinkWriteFormat format) {
		this.format = format;
	}

	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public LinkedHashMap<String, String> getConfig() {
		return config;
	}

	public void setConfig(LinkedHashMap<String, String> config) {
		this.config = config;
	}
}

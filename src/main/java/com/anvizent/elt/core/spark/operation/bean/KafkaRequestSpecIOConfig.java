package com.anvizent.elt.core.spark.operation.bean;

import java.io.Serializable;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class KafkaRequestSpecIOConfig implements Serializable {
	private static final long serialVersionUID = 1L;

	private String topic;
	private KafkaRequestSpecConsumerProperties consumerProperties;
	private Long taskCount;
	private Long replicas;
	private String taskDuration;

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public KafkaRequestSpecConsumerProperties getConsumerProperties() {
		return consumerProperties;
	}

	public void setConsumerProperties(KafkaRequestSpecConsumerProperties consumerProperties) {
		this.consumerProperties = consumerProperties;
	}

	public Long getTaskCount() {
		return taskCount;
	}

	public void setTaskCount(Long taskCount) {
		this.taskCount = taskCount;
	}

	public Long getReplicas() {
		return replicas;
	}

	public void setReplicas(Long replicas) {
		this.replicas = replicas;
	}

	public String getTaskDuration() {
		return taskDuration;
	}

	public void setTaskDuration(String taskDuration) {
		this.taskDuration = taskDuration;
	}

}

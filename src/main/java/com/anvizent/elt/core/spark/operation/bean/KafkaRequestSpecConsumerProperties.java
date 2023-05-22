package com.anvizent.elt.core.spark.operation.bean;

import java.io.Serializable;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class KafkaRequestSpecConsumerProperties implements Serializable {
	private static final long serialVersionUID = 1L;

	private String bootstrapServers;

	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

}

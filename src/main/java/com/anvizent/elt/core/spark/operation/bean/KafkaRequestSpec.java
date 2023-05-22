package com.anvizent.elt.core.spark.operation.bean;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class KafkaRequestSpec implements Serializable {
	private static final long serialVersionUID = 1L;

	private ArrayList<String> dimensions;
	private ArrayList<KafkaRequestMetricsSpec> metricsSpec;
	private KafkaRequestSpecIOConfig ioConfig;

	public ArrayList<String> getDimensions() {
		return dimensions;
	}

	public void setDimensions(ArrayList<String> dimensions) {
		this.dimensions = dimensions;
	}

	public ArrayList<KafkaRequestMetricsSpec> getMetricsSpec() {
		return metricsSpec;
	}

	public void setMetricsSpec(ArrayList<KafkaRequestMetricsSpec> metricsSpec) {
		this.metricsSpec = metricsSpec;
	}

	public KafkaRequestSpecIOConfig getIoConfig() {
		return ioConfig;
	}

	public void setIoConfig(KafkaRequestSpecIOConfig ioConfig) {
		this.ioConfig = ioConfig;
	}
}

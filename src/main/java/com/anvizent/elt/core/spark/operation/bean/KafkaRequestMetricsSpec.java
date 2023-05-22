package com.anvizent.elt.core.spark.operation.bean;

import java.io.Serializable;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class KafkaRequestMetricsSpec implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final String TYPE_DEFAULT = "doubleSum";

	private String name;
	private String fieldName;
	private String type;

	public KafkaRequestMetricsSpec() {
	}

	public KafkaRequestMetricsSpec(String fieldName) {
		this(fieldName, fieldName, null);
	}

	public KafkaRequestMetricsSpec(String fieldName, String type) {
		this(fieldName, fieldName, type);
	}

	public KafkaRequestMetricsSpec(String name, String fieldName, String type) {
		this.name = name;
		this.fieldName = fieldName;
		this.type = type;

		if (type == null) {
			this.type = TYPE_DEFAULT;
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
}

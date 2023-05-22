package com.anvizent.elt.core.spark.operation.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RepartitionConfigBean extends ConfigBean implements SimpleOperationConfigBean {

	private static final long serialVersionUID = 1L;

	private Integer numberOfPartitions;
	private ArrayList<String> keyFields;

	public Integer getNumberOfPartitions() {
		return numberOfPartitions;
	}

	public void setNumberOfPartitions(Integer numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
	}

	public ArrayList<String> getKeyFields() {
		return keyFields;
	}

	public void setKeyFields(ArrayList<String> keyFields) {
		this.keyFields = keyFields;
	}
}

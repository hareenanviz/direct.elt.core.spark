package com.anvizent.elt.core.spark.operation.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.RetryMandatoryConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class MLMLookupConfigBean extends ConfigBean implements SimpleOperationConfigBean, RetryMandatoryConfigBean {

	private static final long serialVersionUID = 1L;

	private String queryURL;
	private String initiateURL;
	private String addToTaskKillListURL;
	private String userId;
	private String clientId;
	private String tableName;
	private boolean incremental;
	private String kafkaTopic;
	private String kafkaBootstrapServers;
	private Long taskCount;
	private Long replicas;
	private String taskDuration;
	private String privateKey;
	private String iv;
	private String druidTimeFormat;
	private ArrayList<String> keyFields;

	public String getQueryURL() {
		return queryURL;
	}

	public void setQueryURL(String queryURL) {
		this.queryURL = queryURL;
	}

	public String getInitiateURL() {
		return initiateURL;
	}

	public void setInitiateURL(String initiateURL) {
		this.initiateURL = initiateURL;
	}

	public String getAddToTaskKillListURL() {
		return addToTaskKillListURL;
	}

	public void setAddToTaskKillListURL(String addToTaskKillListURL) {
		this.addToTaskKillListURL = addToTaskKillListURL;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public boolean isIncremental() {
		return incremental;
	}

	public void setIncremental(boolean incremental) {
		this.incremental = incremental;
	}

	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public void setKafkaTopic(String kafkaTopic) {
		this.kafkaTopic = kafkaTopic;
	}

	public String getKafkaBootstrapServers() {
		return kafkaBootstrapServers;
	}

	public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
		this.kafkaBootstrapServers = kafkaBootstrapServers;
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

	public String getPrivateKey() {
		return privateKey;
	}

	public void setPrivateKey(String privateKey) {
		this.privateKey = privateKey;
	}

	public String getIv() {
		return iv;
	}

	public void setIv(String iv) {
		this.iv = iv;
	}

	public String getDruidTimeFormat() {
		return druidTimeFormat;
	}

	public void setDruidTimeFormat(String druidTimeFormat) {
		this.druidTimeFormat = druidTimeFormat;
	}

	public ArrayList<String> getKeyFields() {
		return keyFields;
	}

	public void setKeyFields(ArrayList<String> keyFields) {
		this.keyFields = keyFields;
	}
}

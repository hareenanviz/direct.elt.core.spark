package com.anvizent.elt.core.spark.operation.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.spark.constant.JoinMode;
import com.anvizent.elt.core.spark.constant.JoinType;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class JoinConfigBean extends ConfigBean implements MultiInputOperationConfigBean {

	private static final long serialVersionUID = 1L;

	private JoinType joinType;
	private JoinMode joinMode;
	private ArrayList<String> lhsFields;
	private ArrayList<String> rhsFields;
	private String lhsPrefix;
	private String rhsPrefix;
	private String maxRowsForBroadcast;
	private String maxSizeForBroadcast;

	public JoinType getJoinType() {
		return joinType;
	}

	public void setJoinType(JoinType joinType) {
		this.joinType = joinType;
	}

	public JoinMode getJoinMode() {
		return joinMode;
	}

	public void setJoinMode(JoinMode joinMode) {
		this.joinMode = joinMode;
	}

	public ArrayList<String> getLHSFields() {
		return lhsFields;
	}

	public void setLHSFields(ArrayList<String> lhsFields) {
		this.lhsFields = lhsFields;
	}

	public ArrayList<String> getRHSFields() {
		return rhsFields;
	}

	public void setRHSFields(ArrayList<String> rhsFields) {
		this.rhsFields = rhsFields;
	}

	public String getLHSPrefix() {
		return lhsPrefix;
	}

	public void setLHSPrefix(String lhsPrefix) {
		this.lhsPrefix = lhsPrefix;
	}

	public String getRHSPrefix() {
		return rhsPrefix;
	}

	public void setRHSPrefix(String rhsPrefix) {
		this.rhsPrefix = rhsPrefix;
	}

	public String getMaxRowsForBroadcast() {
		return maxRowsForBroadcast;
	}

	public void setMaxRowsForBroadcast(String maxRowsForBroadcast) {
		this.maxRowsForBroadcast = maxRowsForBroadcast;
	}

	public String getMaxSizeForBroadcast() {
		return maxSizeForBroadcast;
	}

	public void setMaxSizeForBroadcast(String maxSizeForBroadcast) {
		this.maxSizeForBroadcast = maxSizeForBroadcast;
	}

}

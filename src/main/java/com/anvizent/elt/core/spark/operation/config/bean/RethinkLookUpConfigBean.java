package com.anvizent.elt.core.spark.operation.config.bean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RethinkLookUpConfigBean extends RethinkRetrievalConfigBean {

	private static final long serialVersionUID = 1L;

	private boolean isLimitTo1;

	public boolean isLimitTo1() {
		return isLimitTo1;
	}

	public void setLimitTo1(boolean isLimitTo1) {
		this.isLimitTo1 = isLimitTo1;
	}
}

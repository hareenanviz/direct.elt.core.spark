package com.anvizent.elt.core.spark.operation.config.bean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RethinkFetcherConfigBean extends RethinkRetrievalConfigBean {

	private static final long serialVersionUID = 1L;

	private Integer maxFetchLimit;

	public Integer getMaxFetchLimit() {
		return maxFetchLimit;
	}

	public void setMaxFetchLimit(Integer maxFetchLimit) {
		this.maxFetchLimit = maxFetchLimit;
	}
}

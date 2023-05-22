package com.anvizent.elt.core.spark.sink.config.bean;

import java.io.Serializable;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RetryConfigBean implements Serializable {
	private static final long serialVersionUID = 1L;

	private Integer maxRetryCount;
	private Long retryDelay;

	public Integer getMaxRetryCount() {
		return maxRetryCount;
	}

	public void setMaxRetryCount(Integer maxRetryCount) {
		this.maxRetryCount = maxRetryCount;
	}

	public Long getRetryDelay() {
		return retryDelay;
	}

	public void setRetryDelay(Long retryDelay) {
		this.retryDelay = retryDelay;
	}

	public RetryConfigBean(Integer maxRetryCount, Long retryDelay) {
		this.maxRetryCount = maxRetryCount;
		this.retryDelay = retryDelay;
	}
}

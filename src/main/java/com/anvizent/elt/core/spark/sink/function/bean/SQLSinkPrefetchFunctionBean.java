package com.anvizent.elt.core.spark.sink.function.bean;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkPrefetchFunctionBean extends SQLSinkFunctionBean {
	private static final long serialVersionUID = 1L;

	private Integer prefetchBatchSize;
	private boolean removeOnceUsed = true;
	private Integer maxElementsInMemory;

	public Integer getPrefetchBatchSize() {
		return prefetchBatchSize;
	}

	public void setPrefetchBatchSize(Integer prefetchBatchSize) {
		this.prefetchBatchSize = prefetchBatchSize;
	}

	public boolean isRemoveOnceUsed() {
		return removeOnceUsed;
	}

	public void setRemoveOnceUsed(boolean removeOnceUsed) {
		this.removeOnceUsed = removeOnceUsed;
	}

	public Integer getMaxElementsInMemory() {
		return maxElementsInMemory;
	}

	public void setMaxElementsInMemory(Integer maxElementsInMemory) {
		this.maxElementsInMemory = maxElementsInMemory;
	}

	public SQLSinkPrefetchFunctionBean(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure) {
		super(sqlSinkConfigBean, structure);

		this.prefetchBatchSize = sqlSinkConfigBean.getPrefetchBatchSize();
		this.removeOnceUsed = sqlSinkConfigBean.isRemoveOnceUsed();
		this.maxElementsInMemory = sqlSinkConfigBean.getMaxElementsInMemory();
	}
}
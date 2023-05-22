package com.anvizent.elt.core.spark.operation.service;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import org.apache.spark.TaskContext;

import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.operation.cache.ArangoDBRetrievalCache;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBRetrievalConfigBean;

import net.sf.ehcache.CacheManager;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ArangoDBRetrievalService implements Serializable {

	private static final long serialVersionUID = 1L;

	protected ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean;
	protected ArangoDBRetrievalCache arangoDBRetrievalCache;

	public ArangoDBRetrievalService(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean)
			throws UnimplementedException, ImproperValidationException, TimeoutException, SQLException {
		this.arangoDBRetrievalConfigBean = arangoDBRetrievalConfigBean;
		createConnection();
	}

	public void createConnection() throws TimeoutException, UnimplementedException, ImproperValidationException, SQLException {
		createCache();
	}

	private void createCache() {
		if (arangoDBRetrievalConfigBean.getCacheType().equals(CacheType.EHCACHE)) {
			CacheManager cacheManager = CacheManager.getInstance();
			String cacheName = TaskContext.getPartitionId() + "_" + arangoDBRetrievalConfigBean.getName();

			if (arangoDBRetrievalCache == null || cacheManager.getCache(cacheName) == null) {
				arangoDBRetrievalCache = new ArangoDBRetrievalCache(cacheName, arangoDBRetrievalConfigBean);
				cacheManager.addCache(arangoDBRetrievalCache);
			}
		}
	}
}

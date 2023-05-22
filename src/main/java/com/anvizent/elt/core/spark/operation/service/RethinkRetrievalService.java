package com.anvizent.elt.core.spark.operation.service;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import org.apache.spark.TaskContext;

import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.operation.cache.RethinkRetrievalCache;
import com.anvizent.elt.core.spark.operation.config.bean.RethinkRetrievalConfigBean;

import net.sf.ehcache.CacheManager;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RethinkRetrievalService implements Serializable {

	private static final long serialVersionUID = 1L;

	protected RethinkRetrievalConfigBean rethinkRetrievalConfigBean;
	protected RethinkRetrievalCache rethinkRetrievalCache;

	public RethinkRetrievalService(RethinkRetrievalConfigBean rethinkRetrievalConfigBean)
			throws UnimplementedException, ImproperValidationException, TimeoutException, SQLException {
		this.rethinkRetrievalConfigBean = rethinkRetrievalConfigBean;
		createConnection();
	}

	public void createConnection() throws TimeoutException, UnimplementedException, ImproperValidationException, SQLException {
		createCache();
	}

	private void createCache() {
		if (rethinkRetrievalConfigBean.getCacheType().equals(CacheType.EHCACHE)) {
			CacheManager cacheManager = CacheManager.getInstance();
			String cacheName = TaskContext.getPartitionId() + "_" + rethinkRetrievalConfigBean.getName();

			if (rethinkRetrievalCache == null || cacheManager.getCache(cacheName) == null) {
				rethinkRetrievalCache = new RethinkRetrievalCache(cacheName, rethinkRetrievalConfigBean);
				cacheManager.addCache(rethinkRetrievalCache);
			}
		}
	}
}

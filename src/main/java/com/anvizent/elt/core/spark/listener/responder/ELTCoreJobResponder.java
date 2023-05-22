package com.anvizent.elt.core.spark.listener.responder;

import com.anvizent.elt.core.listener.common.bean.ELTCoreJobListener;

import net.sf.ehcache.CacheManager;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ELTCoreJobResponder implements ELTCoreJobListener {

	@Override
	public void beforeStart() {
	}

	@Override
	public void afterStop() throws Exception {
		CacheManager cacheManager = CacheManager.getInstance();
		cacheManager.clearAll();
		cacheManager.shutdown();
	}

}

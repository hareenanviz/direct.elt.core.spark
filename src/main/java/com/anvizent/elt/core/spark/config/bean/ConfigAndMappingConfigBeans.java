package com.anvizent.elt.core.spark.config.bean;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.constant.Mapping;
import com.anvizent.elt.core.spark.store.StatsStore;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ConfigAndMappingConfigBeans {

	private ConfigBean configBean;
	private LinkedHashMap<Mapping, MappingConfigBean> mappingConfigBeans;
	private StatsStore statsStore;
	private ErrorHandlerSink errorHandlerSink;

	public ConfigBean getConfigBean() {
		return configBean;
	}

	public void setConfigBean(ConfigBean configBean) {
		this.configBean = configBean;
	}

	public LinkedHashMap<Mapping, MappingConfigBean> getMappingConfigBeans() {
		return mappingConfigBeans;
	}

	public void setMappingConfigBeans(LinkedHashMap<Mapping, MappingConfigBean> mappingConfigBeans) {
		this.mappingConfigBeans = mappingConfigBeans;
	}

	public StatsStore getStatsStore() {
		return statsStore;
	}

	public void setStatsStore(StatsStore statsStore) {
		this.statsStore = statsStore;
	}

	public ErrorHandlerSink getErrorHandlerSink() {
		return errorHandlerSink;
	}

	public void setErrorHandlerSink(ErrorHandlerSink errorHandlerSink) {
		this.errorHandlerSink = errorHandlerSink;
	}

}
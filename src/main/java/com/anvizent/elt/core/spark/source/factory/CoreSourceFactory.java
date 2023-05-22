package com.anvizent.elt.core.spark.source.factory;

import java.util.HashMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.source.config.bean.CoreSource;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class CoreSourceFactory extends SourceFactory {

	private static final long serialVersionUID = 1L;

	@Override
	public Component createSourceComponent(ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws ImproperValidationException, UnsupportedException, Exception {
		JavaRDD<HashMap<String, Object>> javaRDD = read(configAndMappingConfigBeans.getConfigBean());

		if (configAndMappingConfigBeans.getConfigBean().isPersist()) {
			javaRDD.persist(StorageLevel.MEMORY_AND_DISK());
		}

		Component component = Component.createComponent(ApplicationBean.getInstance().getSparkSession(), configAndMappingConfigBeans.getConfigBean().getName(),
		        javaRDD, ((CoreSource) configAndMappingConfigBeans.getConfigBean()).getStructType());

		// TODO stats and error handler
		return component;
	}

	protected abstract JavaRDD<HashMap<String, Object>> read(ConfigBean configBean) throws UnsupportedException, ImproperValidationException, Exception;
}

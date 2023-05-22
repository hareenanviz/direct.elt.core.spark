package com.anvizent.elt.core.spark.source.factory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class SQLSourceFactory extends SourceFactory {

	private static final long serialVersionUID = 1L;

	@Override
	public Component createSourceComponent(ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws ImproperValidationException, UnsupportedException, Exception {
		Dataset<Row> dataSet = read(configAndMappingConfigBeans.getConfigBean());

		if (configAndMappingConfigBeans.getConfigBean().isPersist()) {
			dataSet.persist(StorageLevel.MEMORY_AND_DISK());
		}

		Component component = Component.createComponent(ApplicationBean.getInstance().getSparkSession(), configAndMappingConfigBeans.getConfigBean().getName(),
		        dataSet, dataSet.schema(), configAndMappingConfigBeans.getConfigBean(),
		        ApplicationBean.getInstance().getAccumulators(configAndMappingConfigBeans.getConfigBean().getName(), getName()),
		        ErrorHandlerUtil.getErrorHandlerFunction(configAndMappingConfigBeans.getConfigBean(), Component.getStructure(dataSet.schema()),
		                configAndMappingConfigBeans.getErrorHandlerSink(), getName()),
		        ErrorHandlerUtil.getJobDetails(configAndMappingConfigBeans.getConfigBean(), getName()));

		return component;
	}

	protected abstract Dataset<Row> read(ConfigBean configBean) throws UnsupportedException, ImproperValidationException, Exception;
}

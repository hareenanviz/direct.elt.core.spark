package com.anvizent.elt.core.spark.source.factory;

import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.spark.common.util.EitherOr;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.source.pojo.StructTypeAndJavaRDD;
import com.anvizent.elt.core.spark.source.rdd.service.HashMapToHashMapFunction;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class ComplexSourceFactory extends SourceFactory {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("rawtypes")
	@Override
	public Component createSourceComponent(ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws ImproperValidationException, UnsupportedException, Exception {
		EitherOr<Dataset<Row>, StructTypeAndJavaRDD<HashMap>> data = read(configAndMappingConfigBeans.getConfigBean());

		if (configAndMappingConfigBeans.getConfigBean().isPersist()) {
			if (data.getA() != null) {
				data.getA().persist(StorageLevel.MEMORY_AND_DISK());
			} else {
				data.getB().getRdd().persist(StorageLevel.MEMORY_AND_DISK());
			}
		}

		Component component = getComponent(data, configAndMappingConfigBeans);

		return component;
	}

	@SuppressWarnings("rawtypes")
	private Component getComponent(EitherOr<Dataset<Row>, StructTypeAndJavaRDD<HashMap>> data, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws UnsupportedException, InvalidArgumentsException, Exception {
		if (data.getA() != null) {
			return Component.createComponent(ApplicationBean.getInstance().getSparkSession(), configAndMappingConfigBeans.getConfigBean().getName(),
			        data.getA(), data.getA().schema(), configAndMappingConfigBeans.getConfigBean(),
			        ApplicationBean.getInstance().getAccumulators(configAndMappingConfigBeans.getConfigBean().getName(), getName()),
			        ErrorHandlerUtil.getErrorHandlerFunction(configAndMappingConfigBeans.getConfigBean(), Component.getStructure(data.getA().schema()),
			                configAndMappingConfigBeans.getErrorHandlerSink(), getName()),
			        ErrorHandlerUtil.getJobDetails(configAndMappingConfigBeans.getConfigBean(), getName()));
		} else {
			return Component.createComponent(ApplicationBean.getInstance().getSparkSession(), configAndMappingConfigBeans.getConfigBean().getName(),
			        data.getB().getRdd().map(new HashMapToHashMapFunction()), data.getB().getStructType());
		}
	}

	@SuppressWarnings("rawtypes")
	protected abstract EitherOr<Dataset<Row>, StructTypeAndJavaRDD<HashMap>> read(ConfigBean configBean)
	        throws UnsupportedException, ImproperValidationException, UnimplementedException, RecordProcessingException;
}

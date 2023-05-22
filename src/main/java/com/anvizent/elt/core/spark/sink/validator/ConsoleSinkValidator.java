package com.anvizent.elt.core.spark.sink.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink.ConsoleSink;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.source.config.bean.ConsoleSinkConfigBean;
import com.anvizent.elt.core.spark.validator.ErrorHandlerValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class ConsoleSinkValidator extends Validator implements ErrorHandlerValidator {
	public ConsoleSinkValidator(Factory factory) {
		super(factory);
	}

	private static final long serialVersionUID = 1L;

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException {
		ConsoleSinkConfigBean sinkConfigBean = new ConsoleSinkConfigBean();
		sinkConfigBean.setWriteAll(ConfigUtil.getBoolean(configs, ConsoleSink.WRITE_ALL, exception));
		return sinkConfigBean;
	}

	@Override
	public ConfigBean validateAndSetErrorHandler(LinkedHashMap<String, String> configs) throws ImproperValidationException, InvalidConfigException {
		ConsoleSinkConfigBean sinkConfigBean = new ConsoleSinkConfigBean();
		sinkConfigBean.setName(ConfigUtil.getString(configs, General.NAME));
		sinkConfigBean.setWriteAll(true);

		return sinkConfigBean;
	}

	@Override
	public void replaceWithComponentSpecific(ErrorHandlerSink errorHandlerSink, LinkedHashMap<String, String> configs) {
		// TODO Auto-generated method stub
	}

}

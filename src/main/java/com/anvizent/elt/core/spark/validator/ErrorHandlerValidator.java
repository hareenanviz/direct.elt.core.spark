package com.anvizent.elt.core.spark.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;

/**
 * @author Hareen Bejjanki
 *
 */
public interface ErrorHandlerValidator {

	ConfigBean validateAndSetErrorHandler(LinkedHashMap<String, String> configs) throws ImproperValidationException, InvalidConfigException;

	void replaceWithComponentSpecific(ErrorHandlerSink errorHandlerSink, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, InvalidConfigException;
}

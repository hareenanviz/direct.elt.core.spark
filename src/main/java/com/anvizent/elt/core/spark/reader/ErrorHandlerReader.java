package com.anvizent.elt.core.spark.reader;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.SeekDetails;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.store.ErrorHandlerStore;
import com.anvizent.elt.core.spark.config.ComponentConfiguration;
import com.anvizent.elt.core.spark.config.ConfigurationReader;
import com.anvizent.elt.core.spark.config.ConfigurationReaderBuilder;
import com.anvizent.elt.core.spark.config.ConfigurationReadingException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.ErrorHandlers;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.constant.Constants.ARGSConstant;
import com.anvizent.elt.core.spark.exception.ConfigReferenceNotFoundException;
import com.anvizent.elt.core.spark.exception.ErrorHandlersException;
import com.anvizent.elt.core.spark.exception.InvalidConfigurationReaderProperty;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.sink.config.bean.ArangoDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.RethinkDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;
import com.anvizent.elt.core.spark.sink.factory.ArangoDBSinkFactory;
import com.anvizent.elt.core.spark.sink.factory.ConsoleSinkFactory;
import com.anvizent.elt.core.spark.sink.factory.RethinkDBSinkFactory;
import com.anvizent.elt.core.spark.sink.factory.SQLSinkFactory;
import com.anvizent.elt.core.spark.sink.validator.ArangoDBSinkValidator;
import com.anvizent.elt.core.spark.sink.validator.ConsoleSinkValidator;
import com.anvizent.elt.core.spark.sink.validator.RethinkDBSinkValidator;
import com.anvizent.elt.core.spark.sink.validator.SQLSinkValidator;
import com.anvizent.elt.core.spark.source.config.bean.ConsoleSinkConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class ErrorHandlerReader {

	public static ErrorHandlerStore getErrorHandlerStore(LinkedHashMap<String, ArrayList<String>> arguments, LinkedHashMap<String, Factory> errorHandlers)
	        throws UnsupportedEncodingException, ConfigurationReadingException, InvalidConfigurationReaderProperty, ImproperValidationException,
	        UnimplementedException, InvalidConfigException, UnsupportedException, InvalidInputForConfigException, ConfigReferenceNotFoundException {

		if (!arguments.containsKey(ARGSConstant.ERROR_HANDLERS)) {
			return null;
		}

		ConfigurationReader errorHandlersConfigurationReader = new ConfigurationReaderBuilder(arguments, null,
		        arguments.get(ARGSConstant.ERROR_HANDLERS).get(0), ConfigConstants.General.EOR).build();
		ErrorHandlerStore errorHandlerStore = getErrorHandlerStore(errorHandlersConfigurationReader, errorHandlers);

		return errorHandlerStore;
	}

	private static ErrorHandlerStore getErrorHandlerStore(ConfigurationReader errorHandlersConfigurationReader, LinkedHashMap<String, Factory> errorHandlers)
	        throws ErrorHandlersException, ImproperValidationException, UnimplementedException, InvalidConfigException, ConfigurationReadingException,
	        InvalidInputForConfigException, ConfigReferenceNotFoundException {
		ErrorHandlerStore errorHandlerStore = new ErrorHandlerStore();

		ComponentConfiguration componentConfiguration = null;
		ComponentConfiguration defaultResourceComponentConfiguration = null;

		while ((componentConfiguration = errorHandlersConfigurationReader.getNextConfiguration()) != null) {

			if (componentConfiguration.getConfigurationName().equals(ErrorHandlers.DEFAULT_RESOURCE)) {
				defaultResourceComponentConfiguration = componentConfiguration;
				String defaultEHResourceName = getDefaultEHResourceName(componentConfiguration);
				errorHandlerStore.setDefaultEHResourceName(defaultEHResourceName);
			} else {
				if (errorHandlers.containsKey(componentConfiguration.getConfigurationName())) {
					setErrorHandlerConfiguration(errorHandlers.get(componentConfiguration.getConfigurationName()), componentConfiguration, errorHandlerStore);
				} else {
					throw new UnimplementedException("'" + componentConfiguration.getConfigurationName() + "' Error handler is not implemented yet.");
				}
			}
		}

		validateErrorHandlers(errorHandlerStore, defaultResourceComponentConfiguration);

		return errorHandlerStore;
	}

	private static void setErrorHandlerConfiguration(Factory factory, ComponentConfiguration componentConfiguration, ErrorHandlerStore errorHandlerStore)
	        throws ImproperValidationException, InvalidConfigException, UnimplementedException {
		Validator validator = factory.getValidator();
		validator.setExceptionDetails(componentConfiguration.getSeekDetails(), componentConfiguration.getConfiguration());

		if (factory instanceof RethinkDBSinkFactory) {
			RethinkDBSinkConfigBean configBean = (RethinkDBSinkConfigBean) ((RethinkDBSinkValidator) validator)
			        .validateAndSetErrorHandler(componentConfiguration.getConfiguration());
			errorHandlerStore.putEHSink(configBean.getName(), configBean);
		} else if (factory instanceof ArangoDBSinkFactory) {
			ArangoDBSinkConfigBean configBean = (ArangoDBSinkConfigBean) ((ArangoDBSinkValidator) validator)
			        .validateAndSetErrorHandler(componentConfiguration.getConfiguration());
			errorHandlerStore.putEHSink(configBean.getName(), configBean);
		} else if (factory instanceof SQLSinkFactory) {
			SQLSinkConfigBean configBean = (SQLSinkConfigBean) ((SQLSinkValidator) validator)
			        .validateAndSetErrorHandler(componentConfiguration.getConfiguration());
			errorHandlerStore.putEHSink(configBean.getName(), configBean);
		} else if (factory instanceof ConsoleSinkFactory) {
			ConsoleSinkConfigBean configBean = (ConsoleSinkConfigBean) ((ConsoleSinkValidator) validator)
			        .validateAndSetErrorHandler(componentConfiguration.getConfiguration());
			errorHandlerStore.putEHSink(configBean.getName(), configBean);
		} else {
			throw new UnimplementedException();
		}
	}

	private static String getDefaultEHResourceName(ComponentConfiguration componentConfiguration) throws ErrorHandlersException {
		ErrorHandlersException exception = createErrorHandlerException(componentConfiguration.getSeekDetails(), componentConfiguration.getConfigurationName());

		String name = ConfigUtil.getString(componentConfiguration.getConfiguration(), General.NAME);
		if (name == null || name.isEmpty()) {
			exception.add(Message.SINGLE_KEY_MANDATORY, General.NAME);
		}

		if (exception.getNumberOfExceptions() > 0) {
			throw exception;
		}

		return name;
	}

	private static ErrorHandlersException createErrorHandlerException(SeekDetails seekDetails, String componentName) {
		ErrorHandlersException exception = new ErrorHandlersException();
		exception.setComponent(ErrorHandlers.ERROR_HANDLERS);
		exception.setComponentName(componentName);
		exception.setSeekDetails(seekDetails);

		return exception;
	}

	private static void validateErrorHandlers(ErrorHandlerStore errorHandlerStore, ComponentConfiguration componentConfiguration)
	        throws ErrorHandlersException {

		if (errorHandlerStore.getEHSinks() == null || errorHandlerStore.getEHSinks().isEmpty()) {
			throw new ErrorHandlersException(Message.EH_SINK_RESOURCE_NOT_FOUND);
		} else if (componentConfiguration == null) {
			throw new ErrorHandlersException(Message.DEFAULT_RESOURCE_NOT_FOUND);
		} else {
			if (!errorHandlerStore.getEHSinks().containsKey(errorHandlerStore.getDefaultEHResourceName())) {
				ErrorHandlersException exception = createErrorHandlerException(componentConfiguration.getSeekDetails(),
				        componentConfiguration.getConfigurationName());
				exception.add(Message.RESOURCE_NOT_FOUND, errorHandlerStore.getDefaultEHResourceName());

				throw exception;
			}
		}
	}

}

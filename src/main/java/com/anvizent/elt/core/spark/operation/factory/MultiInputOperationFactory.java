package com.anvizent.elt.core.spark.operation.factory;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.CoerceException;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.constant.CounterType;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.exception.SingleInputNotSupportedException;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class MultiInputOperationFactory extends OperationFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected Component baseProcess(ConfigBean configBean, LinkedHashMap<String, Component> components, ErrorHandlerSink errorHandlerSink) throws Exception {
		if (configBean.getSources().size() < getMinInputs() || (getMaxInputs() == null ? false : configBean.getSources().size() > getMaxInputs())) {
			throw new SingleInputNotSupportedException("Single input is not supported.");
		}

		validateSchema(configBean, components);

		return process(configBean, components, errorHandlerSink);
	}

	private void validateSchema(ConfigBean configBean, LinkedHashMap<String, Component> components) throws InvalidConfigException, ImproperValidationException,
	        UnimplementedException, ClassNotFoundException, SQLException, InvalidInputForConfigException, TimeoutException, DateParseException,
	        UnsupportedCoerceException, InvalidSituationException, CoerceException, InvalidConfigValueException {
		SchemaValidator schemaValidator = getSchemaValidator();

		for (int i = 0; i < configBean.getSources().size(); i++) {
			if (schemaValidator != null) {
				InvalidConfigException invalidConfigException = new InvalidConfigException();
				invalidConfigException.setDetails(configBean);
				schemaValidator.validate(configBean, i, components.get(configBean.getSources().get(i)).getStructure(), invalidConfigException);

				if (invalidConfigException.getNumberOfExceptions() > 0) {
					throw invalidConfigException;
				}
			}
		}
	}

	@Override
	protected void countInputs(ConfigBean configBean, LinkedHashMap<String, Component> components) throws Exception {
		count(CounterType.INPUT, configBean, components);
	}

	protected abstract Component process(ConfigBean configBean, LinkedHashMap<String, Component> components, ErrorHandlerSink errorHandlerSink)
	        throws Exception;
}

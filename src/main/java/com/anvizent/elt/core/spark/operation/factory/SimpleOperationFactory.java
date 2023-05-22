package com.anvizent.elt.core.spark.operation.factory;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.constant.CounterType;
import com.anvizent.elt.core.spark.exception.MultipleInputsNotSupportedException;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class SimpleOperationFactory extends OperationFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected Component baseProcess(ConfigBean configBean, LinkedHashMap<String, Component> components, ErrorHandlerSink errorHandlerSink) throws Exception {
		if (configBean.getSources().size() < getMinInputs() || configBean.getSources().size() > getMaxInputs()) {
			throw new MultipleInputsNotSupportedException("More than one input is not supported.");
		}

		Component inputComponent = components.get(configBean.getSource());
		SchemaValidator schemaValidator = getSchemaValidator();

		if (schemaValidator != null) {
			InvalidConfigException invalidConfigException = new InvalidConfigException();
			invalidConfigException.setDetails(configBean);
			schemaValidator.validate(configBean, -1, inputComponent.getStructure(), invalidConfigException);
			if (invalidConfigException.getNumberOfExceptions() > 0) {
				throw invalidConfigException;
			}
		}

		return process(configBean, inputComponent, errorHandlerSink);
	}

	@Override
	protected void countInputs(ConfigBean configBean, LinkedHashMap<String, Component> components) {
		Component inputComponent = components.get(configBean.getSource());
		count(CounterType.INPUT, inputComponent);
	}

	protected abstract Component process(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception;
}

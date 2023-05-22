package com.anvizent.elt.core.spark.filter.factory;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Filter.Components;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.filter.config.bean.FilterByRegExConfigBean;
import com.anvizent.elt.core.spark.filter.doc.helper.FilterByRegExDocHelper;
import com.anvizent.elt.core.spark.filter.service.FilterService;
import com.anvizent.elt.core.spark.filter.validator.FilterByRegExValidator;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FilterByRegExFactory extends FilterByExpressionFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected Component process(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		FilterByRegExConfigBean regExConfigBean = (FilterByRegExConfigBean) configBean;
		FilterService.buildRegularExpressions(regExConfigBean);

		return super.process(regExConfigBean, component, errorHandlerSink);
	}

	@Override
	public String getName() {
		return Components.FILTER_BY_REGULAR_EXPRESSION.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new FilterByRegExDocHelper(this);
	}

	@Override
	public Validator getValidator() {
		return new FilterByRegExValidator(this);
	}

	@Override
	public SchemaValidator getSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
	}
}

package com.anvizent.elt.core.spark.operation.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.ResultFetcher;
import com.anvizent.elt.core.spark.constant.HelpConstants;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ResultFetcherDocHelper extends DocHelper {

	public ResultFetcherDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "This component is to help Java handson users. This takes class, method and its arguments if any."
		        + " It calls the given method and captures the result returned by it." };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(ResultFetcher.CLASS_NAMES, General.YES, "",
		        new String[] { "Fully qualified class names for calling methods in it." }, HelpConstants.Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(ResultFetcher.METHOD_NAMES, General.YES, "", new String[] { "Names of the method to be called." },
		        "Number of " + ResultFetcher.METHOD_NAMES + " provided should be equals to number of " + ResultFetcher.CLASS_NAMES + ".",
		        HelpConstants.Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(ResultFetcher.VAR_ARGS_INDEXES, General.NO, "",
		        new String[] { "In case of ambiguous var args methods, this index tell from where the var args start.",
		                "For example get(String, String...), get(String...) are ambiguous var args methods." },
		        "If provided number of " + ResultFetcher.VAR_ARGS_INDEXES + " provided should be equals to number of " + ResultFetcher.CLASS_NAMES + ".",
		        HelpConstants.Type.LIST_OF_INTEGERS);

		configDescriptionUtil.addConfigDescription(ResultFetcher.METHOD_ARGUMENT_FIELDS, General.YES + ": If method singnature need arguments.", "",
		        new String[] { "Method argument field names." },
		        "Number of " + ResultFetcher.METHOD_ARGUMENT_FIELDS + " provided should be equals to number of " + ResultFetcher.CLASS_NAMES + ".",
		        HelpConstants.Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(ResultFetcher.RETURN_FIELDS, General.NO, "", new String[] { "Return field's names." },
		        "Number of " + ResultFetcher.RETURN_FIELDS + " provided should be equals to number of " + ResultFetcher.METHOD_NAMES + ".",
		        HelpConstants.Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(ResultFetcher.RETURN_FIELDS_INDEXES, General.NO, "",
		        new String[] { "Order of the return field's resulting data." },
		        "Number of " + ResultFetcher.RETURN_FIELDS_INDEXES + " provided should be equals to number of " + ResultFetcher.METHOD_NAMES + ".",
		        HelpConstants.Type.LIST_OF_INTEGERS);
	}
}

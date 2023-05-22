package com.anvizent.elt.core.spark.filter.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.Filter;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.ResultFetcher;
import com.anvizent.elt.core.spark.constant.HelpConstants;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FilterByResultDocHelper extends DocHelper {
	public FilterByResultDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "Filter by single or multiple result fetcher external methods." };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(Filter.General.EMIT_STREAM_NAMES, General.YES + ": If more than one stream needs to emit.", "",
				new String[] { "Emit stream names" },
				"Number of " + Filter.General.EMIT_STREAM_NAMES + " should be equal to or one more than number of " + ResultFetcher.METHOD_NAMES + ".",
				Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(ResultFetcher.CLASS_NAMES, General.YES, "",
				new String[] { "Fully qualified class names for calling methods in it." }, HelpConstants.Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(ResultFetcher.METHOD_NAMES, General.YES, "", new String[] { "Names of the method to be called." },
				"Number of " + ResultFetcher.METHOD_NAMES + " provided should be equals to number of " + ResultFetcher.CLASS_NAMES + ".",
				HelpConstants.Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(ResultFetcher.METHOD_ARGUMENT_FIELDS, General.YES + ": If method singnature need arguments.", "",
				new String[] { "Method argument field names." },
				"Number of " + ResultFetcher.METHOD_ARGUMENT_FIELDS + " provided should be equals to number of " + ResultFetcher.CLASS_NAMES + ".",
				HelpConstants.Type.LIST_OF_STRINGS);
	}
}

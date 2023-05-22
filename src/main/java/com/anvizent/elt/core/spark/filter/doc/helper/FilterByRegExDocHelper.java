package com.anvizent.elt.core.spark.filter.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.Filter;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Filter.RegularExpression;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FilterByRegExDocHelper extends DocHelper {

	public FilterByRegExDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "Filter by single or multiple regular expression(s)." };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(Filter.General.EMIT_STREAM_NAMES,
				General.YES + ": If more than one stream needs to emit.", "", new String[] { "Emit stream names" }, "Number of "
						+ Filter.General.EMIT_STREAM_NAMES + " should be equal to or one more than number of " + RegularExpression.REGULAR_EXPRESSIONS + ".",
				Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(RegularExpression.REGULAR_EXPRESSIONS, General.YES, "", new String[] { "Filter By Regular Expressions" }, "",
				Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(RegularExpression.FIELDS, General.YES, "", new String[] { "Fields to check with the Regular Expressions" },
				"Number of " + RegularExpression.REGULAR_EXPRESSIONS + " should be equal to the number of " + RegularExpression.FIELDS + ".",
				Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(RegularExpression.IGNORE_ROW_IF_NULL, General.NO, "",
				new String[] { "Ignore the row to filter if null value." }, "", "Boolean");
	}
}

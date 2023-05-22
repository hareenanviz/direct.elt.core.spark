package com.anvizent.elt.core.spark.filter.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.Filter;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.util.StringUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FilterByExpressionDocHelper extends DocHelper {
	public FilterByExpressionDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "Filter By Expressions" };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(Filter.General.EMIT_STREAM_NAMES, General.YES + ": If more than one stream needs to emit.", "",
				new String[] { "Emit stream names" },
				"Number of " + Filter.General.EMIT_STREAM_NAMES + " should be equal to or one more than number of " + Operation.General.EXPRESSIONS + ".",
				Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(Operation.General.EXPRESSIONS, General.YES, "", new String[] { "Filter By Expressions" }, "",
				Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(Operation.General.ARGUMENT_FIELDS, General.YES, "", new String[] { "Argument Fields for the Expressions" },
				"", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(Operation.General.ARGUMENT_TYPES, General.YES, "",
				StringUtil.join(new String[] { "Argument Types of the Expressions" }, General.Type.ALLOWED_TYPES_AS_STRINGS),
				"Number of " + Operation.General.ARGUMENT_FIELDS + " provided should be equals to number of " + Operation.General.ARGUMENT_TYPES + ".",
				Type.LIST_OF_TYPES, true, HTMLTextStyle.ORDERED_LIST);
	}
}

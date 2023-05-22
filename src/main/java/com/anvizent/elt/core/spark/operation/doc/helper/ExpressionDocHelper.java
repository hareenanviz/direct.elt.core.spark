package com.anvizent.elt.core.spark.operation.doc.helper;

import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.constant.HelpConstants;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Expression;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.util.StringUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ExpressionDocHelper extends DocHelper {
	public ExpressionDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "This component is to help Java handson users. This takes a java expression which can be a arthemetic expression or a constant "
				+ "or a method(method can also be user defined) call." };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(Operation.General.EXPRESSIONS, General.YES, "", new String[] { "Expression" });
		configDescriptionUtil.addConfigDescription(Expression.EXPRESSIONS_FIELD_NAMES, General.YES, "", new String[] { "Resultant Expression Field Names" },
				"Number of " + Operation.General.EXPRESSIONS + " provided should be equals to number of " + Expression.EXPRESSIONS_FIELD_NAMES + ".");
		configDescriptionUtil.addConfigDescription(
				Expression.EXPRESSIONS_FIELD_NAMES_INDEXES, General.NO, "", new String[] { "Specifies the order of the resultant expression" }, "Number of "
						+ Expression.EXPRESSIONS_FIELD_NAMES_INDEXES + " provided should be equals to number of " + Expression.EXPRESSIONS_FIELD_NAMES + ".",
				HelpConstants.Type.LIST_OF_INTEGERS);
		configDescriptionUtil.addConfigDescription(Operation.General.ARGUMENT_FIELDS, General.YES, "", new String[] { "Argument Fields for the Expression" });
		configDescriptionUtil.addConfigDescription(Operation.General.ARGUMENT_TYPES, General.YES, "",
				StringUtil.join(new String[] { "Argument Types of the Expression" }, General.Type.ALLOWED_TYPES_AS_STRINGS),
				"Number of " + Operation.General.ARGUMENT_FIELDS + " provided should be equals to number of " + Operation.General.ARGUMENT_TYPES + ".", true,
				HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil.addConfigDescription(Expression.RETURN_TYPES, General.YES, "",
				StringUtil.join(new String[] { "Return Type of the Expression is being Specified" }, General.Type.ALLOWED_TYPES_AS_STRINGS),
				"Number of " + Operation.General.EXPRESSIONS + " provided should be equals to number of " + Expression.RETURN_TYPES + ".", true,
				HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil
				.addConfigDescription(General.DECIMAL_PRECISIONS, "If " + Expression.RETURN_TYPES + " contains " + General.Type.BIG_DECIMAL_AS_STRING, "",
						new String[] {
								"For providing " + General.DECIMAL_PRECISIONS + " for " + General.Type.BIG_DECIMAL_AS_STRING + " " + Expression.RETURN_TYPES },
						"Number of " + General.DECIMAL_PRECISIONS + " provided should be equals to number of " + Expression.RETURN_TYPES + ".",
						HelpConstants.Type.LIST_OF_INTEGERS);
		configDescriptionUtil.addConfigDescription(General.DECIMAL_SCALES, "If " + Expression.RETURN_TYPES + " contains " + General.Type.BIG_DECIMAL_AS_STRING,
				"", new String[] { "For providing " + General.DECIMAL_SCALES + " for " + General.Type.BIG_DECIMAL_AS_STRING + " " + Expression.RETURN_TYPES },
				"Number of " + General.DECIMAL_SCALES + " provided should be equals to number of " + Expression.RETURN_TYPES + ".",
				HelpConstants.Type.LIST_OF_INTEGERS);
	}
}

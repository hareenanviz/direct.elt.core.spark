package com.anvizent.elt.core.spark.mapping.doc.helper;

import com.anvizent.elt.core.spark.constant.CleansingValidationType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.ConditionalReplacementCleansing;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.ConditionalReplacementCleansing.CustomJavaExpressionCleansing;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.ConditionalReplacementCleansing.EqualsNotEqualsCleansing;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.ConditionalReplacementCleansing.MatchesNotMatchesRegexCleansing;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.ConditionalReplacementCleansing.RangeCleansing;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.util.StringUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ConditionalReplacementCleansingDocHelper extends MappingDocHelper {

	public ConditionalReplacementCleansingDocHelper() throws InvalidParameter {
		super();
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(ConditionalReplacementCleansing.FIELDS, General.YES, "",
		        new String[] { "Field names to perform cleansing." }, "", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(ConditionalReplacementCleansing.VALIDATION_TYPES, General.YES, "",
		        new String[] { "Cleansing validation types: ", CleansingValidationType.RANGE.name() + ": Range cleansing from min to max.",
		                CleansingValidationType.EQUAL.name() + ": Equal to cleansing.", CleansingValidationType.NOT_EQUAL.name() + ": Not Equal to cleansing.",
		                CleansingValidationType.EMPTY.name() + ": Empty field cleansing",
		                CleansingValidationType.NOT_EMPTY.name() + ": Not Empty field cleansing",
		                CleansingValidationType.MATCHES_REGEX.name() + ": Field satisfies regular expression cleansing",
		                CleansingValidationType.NOT_MATCHES_REGEX.name() + ": Field does not satisfies regular expression cleansing",
		                CleansingValidationType.CUSTOM_EXPRESSION.name() + ": Custom regular expression cleansing" },
		        true, HTMLTextStyle.ORDERED_LIST, "Number of validation types should be equal to number of fields.", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(ConditionalReplacementCleansing.REPLACEMENT_VALUES,
		        General.YES + ": When " + Operation.General.ON_ZERO_FETCH + " is '" + OnZeroFetchOperation.INSERT + "' AND '"
		                + ConditionalReplacementCleansing.REPLACEMENT_VALUES_BY_FIELDS + "' is not present",
		        "", new String[] { "Replacement values for cleansing." }, "Number of " + ConditionalReplacementCleansing.REPLACEMENT_VALUES
		                + " should be equal to number of " + ConditionalReplacementCleansing.FIELDS + ".",
		        Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(ConditionalReplacementCleansing.REPLACEMENT_VALUES_BY_FIELDS,
		        General.YES + ": When " + Operation.General.ON_ZERO_FETCH + " is '" + OnZeroFetchOperation.INSERT + "' AND '"
		                + ConditionalReplacementCleansing.REPLACEMENT_VALUES + "' is not present",
		        "", new String[] { "Insert values for the select fields using other fields" },
		        "Number of insert values and insert fields in total should match number of select fields", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(RangeCleansing.MIN, General.YES + ": For Range Validation cleansing", "",
		        new String[] { "Minimum values criteria for range validation cleansing." },
		        "Number of " + RangeCleansing.MIN + " should be equal to number of " + ConditionalReplacementCleansing.FIELDS + ".", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(RangeCleansing.MAX, General.YES + ": For Range Validation cleansing", "",
		        new String[] { "Maximum values criteria for range validation cleansing." },
		        "Number of " + RangeCleansing.MAX + " should be equal to number of " + ConditionalReplacementCleansing.FIELDS + ".", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(EqualsNotEqualsCleansing.EQUALS, General.YES + ": For Equals cleansing", "",
		        new String[] { "Equal to values." },
		        "Number of " + EqualsNotEqualsCleansing.EQUALS + " should be equal to number of " + ConditionalReplacementCleansing.FIELDS + ".",
		        Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(EqualsNotEqualsCleansing.NOT_EQUALS, General.YES + ": For Not Equals cleansing", "",
		        new String[] { "Not Equal to values." },
		        "Number of " + EqualsNotEqualsCleansing.NOT_EQUALS + " should be equal to number of " + ConditionalReplacementCleansing.FIELDS + ".",
		        Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(MatchesNotMatchesRegexCleansing.MATCHES_REGEX, General.YES + ": For Matches Regular Expression cleansing",
		        "", new String[] { "Regular expressions to check matched fields." },
		        "Number of " + MatchesNotMatchesRegexCleansing.MATCHES_REGEX + " should be equal to number of " + ConditionalReplacementCleansing.FIELDS + ".",
		        Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(MatchesNotMatchesRegexCleansing.NOT_MATCHES_REGEX,
		        General.YES + ": For Not Matches Regular Expression cleansing", "", new String[] { "Regular expressions to check not matched fields." },
		        "Number of " + MatchesNotMatchesRegexCleansing.NOT_MATCHES_REGEX + " should be equal to number of " + ConditionalReplacementCleansing.FIELDS
		                + ".",
		        Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(CustomJavaExpressionCleansing.EXPRESSIONS, General.YES + ": For Custom Java Expression cleansing", "",
		        new String[] { "Custom Java Regular expressions." },
		        "Number of " + CustomJavaExpressionCleansing.EXPRESSIONS + " should be equal to number of " + ConditionalReplacementCleansing.FIELDS + ".",
		        Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(CustomJavaExpressionCleansing.ARGUMENT_FIELDS, General.YES + ": For Custom Java Expression cleansing", "",
		        new String[] { "Argument Fields for the Expressions." }, "", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(CustomJavaExpressionCleansing.ARGUMENT_TYPES, General.YES + ": For Custom Java Expression cleansing", "",
		        StringUtil.join(new String[] { "Argument Types of the Expressions" }, General.Type.ALLOWED_TYPES_AS_STRINGS), "Number of "
		                + CustomJavaExpressionCleansing.ARGUMENT_TYPES + " should be equal to number of " + CustomJavaExpressionCleansing.ARGUMENT_FIELDS + ".",
		        Type.LIST_OF_TYPES, true, HTMLTextStyle.ORDERED_LIST);

	}

}

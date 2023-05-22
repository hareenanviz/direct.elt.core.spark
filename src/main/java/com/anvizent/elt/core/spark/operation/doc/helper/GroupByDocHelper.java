package com.anvizent.elt.core.spark.operation.doc.helper;

import com.anvizent.elt.core.spark.constant.Aggregations;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.GroupBy;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class GroupByDocHelper extends DocHelper {

	public GroupByDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "This component perform aggregations such as " + Aggregations.SUM.name() + ", " + Aggregations.AVG.name() + ", "
		        + Aggregations.MIN.name() + ", " + Aggregations.MAX.name() + ", " + Aggregations.COUNT.name() + ", " + Aggregations.COUNT_WITH_NULLS.name()
		        + ", by grouping its source with the given '" + GroupBy.GROUP_BY_FIELDS + "'" };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(GroupBy.GROUP_BY_FIELDS, General.YES, "", new String[] { "Fields to perform group by operation." }, "",
		        Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(GroupBy.GROUP_BY_FIELDS_POSITIONS, General.NO, "At the begining",
		        new String[] { "Positions to keep group by fields." }, "Number of positions should be equal to number of group by fields privided.",
		        Type.LIST_OF_INTEGERS);

		configDescriptionUtil.addConfigDescription(GroupBy.AGGREGATIONS, General.YES, "",
		        new String[] { "Aggregations to perform with group by. Below are the allowed aggregaion types:", Aggregations.SUM.name(),
		                Aggregations.AVG.name(), Aggregations.MIN.name(), Aggregations.MAX.name(), Aggregations.COUNT.name(),
		                Aggregations.COUNT_WITH_NULLS.name(), Aggregations.RANDOM.name(), Aggregations.JOIN_BY_DELIM.name() },
		        true, HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil.addConfigDescription(GroupBy.JOIN_AGGREGATION_DELIMETERS,
		        General.YES + " if there are any " + Aggregations.JOIN_BY_DELIM.name() + " aggregations in config" + GroupBy.AGGREGATIONS, "",
		        new String[] { "Field names on which aggregations to be performed." },
		        "Number of aggregation fields should be equal to number of aggregations provided.", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(GroupBy.AGGREGATION_FIELDS, General.YES, "",
		        new String[] { "Field names on which aggregations to be performed." },
		        "Number of aggregation fields should be equal to number of aggregations provided.", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(GroupBy.AGGREGATION_FIELD_ALIAS_NAMES, General.YES, "",
		        new String[] { "Alias names for aggregation fields." }, "Number of aggregation fields should be equal to number of aggregations provided.",
		        Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(GroupBy.AGGREGATION_FIELDS_POSITIONS, General.NO, "After group by fields",
		        new String[] { "Positions to keep aggregation fields." }, "Number of aggregation fields should be equal to number of aggregations provided.",
		        Type.LIST_OF_INTEGERS);
		configDescriptionUtil.addConfigDescription(General.DECIMAL_PRECISIONS, General.NO, String.valueOf(General.DECIMAL_PRECISION),
		        new String[] { "Precisions for average type field." }, "", "Integer");
		configDescriptionUtil.addConfigDescription(General.DECIMAL_SCALES, General.NO, String.valueOf(General.DECIMAL_SCALE),
		        new String[] { "Scales for average type field." }, "", "Integer");
	}
}

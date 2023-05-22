package com.anvizent.elt.core.spark.mapping.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.DateAndTimeGranularity;
import com.anvizent.elt.core.spark.constant.Granularity;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.exception.InvalidParameter;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class DateAndTimeGranularityDocHelper extends MappingDocHelper {

	public DateAndTimeGranularityDocHelper() throws InvalidParameter {
		super();
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(DateAndTimeGranularity.FIELDS, General.YES, "",
				new String[] { "Field names to perform Date And Time Granularity Cleansing." }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(DateAndTimeGranularity.GRANULARITIES, General.YES, "",
				new String[] { "Below are the types of granularities allowed: ", Granularity.SECOND.name(), Granularity.MINUTE.name(),
						Granularity.HOUR.name(), Granularity.DAY.name(), Granularity.MONTH.name(), Granularity.YEAR.name() },
				true, HTMLTextStyle.ORDERED_LIST,
				"Number of " + DateAndTimeGranularity.GRANULARITIES + " should be equal to number of " + DateAndTimeGranularity.GRANULARITIES + ".",
				Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(DateAndTimeGranularity.ALL_DATE_FIELDS, General.NO, "false",
				new String[] { "Adds provided granularity to all the Date fields if set to true." }, "", "Boolean");
	}

}

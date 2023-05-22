package com.anvizent.elt.core.spark.mapping.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Coerce;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.util.StringUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class CoerceDocHelper extends MappingDocHelper {

	public CoerceDocHelper() throws InvalidParameter {
		super();
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(Coerce.COERCE_FIELDS, General.YES, "",
				new String[] { "List of fields whose data types need to be changed." }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(Coerce.COERCE_TO_TYPE, General.YES, "",
				StringUtil.join(new String[] { "Comma separated to which the coercion to done,Types allowed are " }, General.Type.ALLOWED_TYPES_AS_STRINGS),
				true, HTMLTextStyle.ORDERED_LIST, "Number of coerce to types should be equal to number of coerce fields.", Type.LIST_OF_TYPES);
		configDescriptionUtil.addConfigDescription(Coerce.COERCE_TO_FORMAT, "If there is coercion from date to string or string to date.", "",
				new String[] { "Comma separated to date format where required, make sure that index retain." },
				"Number of coerce to formats should be equal to number of coerce fields.", Type.LIST_OF_DATE_FORMATS);
		configDescriptionUtil.addConfigDescription(Coerce.COERCE_DECIMAL_PRECISION, General.NO, String.valueOf(General.DECIMAL_PRECISION),
				new String[] { "Precisions for decimal type field." }, "Number of decimal precisions should be equal to number of constant fields.",
				Type.LIST_OF_INTEGERS);
		configDescriptionUtil.addConfigDescription(Coerce.COERCE_DECIMAL_SCALE, General.NO, String.valueOf(General.DECIMAL_SCALE),
				new String[] { "Scales for decimal type field." }, "Number of deciaml scales should be equal to number of constant fields.",
				Type.LIST_OF_INTEGERS);
	}
}
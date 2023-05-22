package com.anvizent.elt.core.spark.mapping.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Constant;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.util.StringUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ConstantsDocHelper extends MappingDocHelper {

	public ConstantsDocHelper() throws InvalidParameter {
		super();
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(Constant.CONSTANT_FIELDS, General.YES, "",
				new String[] { "List of constants field names need to be added." }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(Constant.CONSTANT_FIELDS_TYPES, General.YES, "",
				StringUtil.join(new String[] { "Eqivalent data types of the constant fields. Types allowed are " }, General.Type.ALLOWED_TYPES_AS_STRINGS),
				true, HTMLTextStyle.ORDERED_LIST, "Number of constant field types should be equal to number of constant fields.", Type.LIST_OF_TYPES);
		configDescriptionUtil.addConfigDescription(Constant.CONSTANT_FIELDS_FORMATS, General.YES, "", new String[] { "Formats of the constant fields." },
				"Number of constant field formats should be equal to number of constant fields.", Type.LIST_OF_DATE_FORMATS);
		configDescriptionUtil.addConfigDescription(Constant.CONSTANT_FIELDS_VALUES, General.YES, "", new String[] { "Values of the constant fields." },
				"Number of constant field values should be equal to number of constant fields.", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(Constant.CONSTANT_FIELDS_POSITIONS, General.YES, "", new String[] { "Positions of the constant fields." },
				"Number of constant field positions should be equal to number of constant fields.", Type.LIST_OF_INTEGERS);
		configDescriptionUtil.addConfigDescription(Constant.CONSTANT_DECIMAL_PRECISION, General.NO, String.valueOf(General.DECIMAL_PRECISION),
				new String[] { "Precisions for decimal type field." }, "Number of decimal precisions should be equal to number of constant fields.",
				Type.LIST_OF_INTEGERS);
		configDescriptionUtil.addConfigDescription(Constant.CONSTANT_DECIMAL_SCALE, General.NO, String.valueOf(General.DECIMAL_SCALE),
				new String[] { "Scales for decimal type field." }, "Number of deciaml scales should be equal to number of constant fields.",
				Type.LIST_OF_INTEGERS);
	}
}

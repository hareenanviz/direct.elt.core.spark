package com.anvizent.elt.core.spark.operation.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Join;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.constant.JoinType;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class JoinDocHelper extends DocHelper {

	public JoinDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "This components joins two sources(left hand, right hand) where '" + Join.LEFT_HAND_SIDE_FIELDS + "' values matches '"
				+ Join.RIGHT_HAND_SIDE_FIELDS + "'" };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil
				.addConfigDescription(
						Join.JOIN_TYPE, General.YES, "", new String[] { "Below are the types of join allowed: ", JoinType.SIMPLE_JOIN.name(),
								JoinType.LEFT_OUTER_JOIN.name(), JoinType.RIGHT_OUTER_JOIN.name(), JoinType.FULL_OUTER_JOIN.name() },
						true, HTMLTextStyle.ORDERED_LIST);

		configDescriptionUtil.addConfigDescription(Join.LEFT_HAND_SIDE_FIELDS, General.YES, "", new String[] { "Fields to join from first source." },
				"Number of fields from left hand side source should match with number of fields from right hand side source.", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(Join.RIGHT_HAND_SIDE_FIELDS, General.YES, "", new String[] { "Fields to join from second source." },
				"Number of fields from right hand side source should match with number of fields from left hand side source.", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(Join.LEFT_HAND_SIDE_FIELD_PREFIX, General.NO, "",
				new String[] { "Prefix for left hand side source fields in case of duplicates." });

		configDescriptionUtil.addConfigDescription(Join.RIGHT_HAND_SIDE_FIELD_PREFIX, General.NO, "",
				new String[] { "Prefix for right hand side source fields in case of duplicates." });
	}

}

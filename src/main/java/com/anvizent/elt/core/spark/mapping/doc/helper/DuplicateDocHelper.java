package com.anvizent.elt.core.spark.mapping.doc.helper;

import com.anvizent.elt.core.spark.constant.AppendAt;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Duplicate;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.exception.InvalidParameter;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class DuplicateDocHelper extends MappingDocHelper {

	public DuplicateDocHelper() throws InvalidParameter {
		super();
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(Duplicate.DUPLICATE_MEASURE_AS_STRING, General.NO, "false",
				new String[] { "Duplicate every measure as string true/false." }, "", "Boolean");
		configDescriptionUtil.addConfigDescription(Duplicate.DUPLICATE_MEASURE_AS_STRING_PREFIX, General.YES + ": If suffix is not provided", "",
				new String[] { "Duplicate field prefix" }, "", Type.STRING);
		configDescriptionUtil.addConfigDescription(Duplicate.DUPLICATE_MEASURE_AS_STRING_SUFFIX, General.YES + ": If prefix is not provided", "",
				new String[] { "Duplicate field suffix" }, "", Type.STRING);
		configDescriptionUtil.addConfigDescription(Duplicate.DUPLICATE_MEASURE_AS_STRING_APPEND_AT, General.NO, AppendAt.END.name(),
				new String[] { "Below are the appent at positions allowed:", AppendAt.BEGIN.name() + ": Appends every duplicate fields at the begining.",
						AppendAt.BEFORE.name() + ": Appends duplicate field before the respective measure.",
						AppendAt.NEXT.name() + ": Appends duplicate field next to the respective measure.",
						AppendAt.END.name() + ": Appends every duplicate field at the end." },
				"", Type.STRING);
	}
}

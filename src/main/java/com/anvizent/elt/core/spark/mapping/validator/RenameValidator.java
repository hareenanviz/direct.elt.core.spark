package com.anvizent.elt.core.spark.mapping.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Rename;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.mapping.config.bean.RenameConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RenameValidator extends MappingValidator {

	private static final long serialVersionUID = 1L;

	@Override
	public MappingConfigBean validateAndSetBean(LinkedHashMap<String, String> configs) throws ImproperValidationException {
		ArrayList<String> renameFrom = ConfigUtil.getArrayList(configs, Rename.RENAME_FROM, exception);
		ArrayList<String> renameTo = ConfigUtil.getArrayList(configs, Rename.RENAME_TO, exception);

		if (renameFrom == null || renameFrom.isEmpty()) {
			addException(Message.SINGLE_KEY_MANDATORY, Rename.RENAME_FROM);
		}
		if (renameTo == null || renameTo.isEmpty()) {
			addException(Message.SINGLE_KEY_MANDATORY, Rename.RENAME_TO);
		}
		if (renameFrom != null && renameTo != null) {
			if (renameFrom.size() != renameTo.size()) {
				addException(Message.SIZE_SHOULD_MATCH, Rename.RENAME_FROM, Rename.RENAME_TO);
			}
		}

		return new RenameConfigBean(renameFrom, renameTo);
	}

	@Override
	public String[] getConfigsList() throws ImproperValidationException {
		return new String[] { Rename.RENAME_FROM, Rename.RENAME_TO };
	}

}

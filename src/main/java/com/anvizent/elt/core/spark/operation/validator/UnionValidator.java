package com.anvizent.elt.core.spark.operation.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Union;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.UnionConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class UnionValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public UnionValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {
		UnionConfigBean configBean = new UnionConfigBean();

		ArrayList<String> sources = ConfigUtil.getArrayList(configs, General.SOURCE, exception);
		String structureSourceName = ConfigUtil.getString(configs, Union.STRUCTURE_SOURCE_NAME);

		if (structureSourceName != null && !sources.contains(structureSourceName)) {
			exception.add(Message.IS_NOT_PRESENT, Union.STRUCTURE_SOURCE_NAME, General.SOURCE);
		}

		configBean.setStructureSourceName(structureSourceName);

		return configBean;
	}

}

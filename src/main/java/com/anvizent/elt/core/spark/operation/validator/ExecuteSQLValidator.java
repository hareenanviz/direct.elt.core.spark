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
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.ExecuteSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.ExecuteSQLConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class ExecuteSQLValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public ExecuteSQLValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {
		ExecuteSQLConfigBean configBean = new ExecuteSQLConfigBean();

		validateAndSetConfigBean(configBean, configs);

		return configBean;
	}

	private void validateAndSetConfigBean(ExecuteSQLConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {

		ArrayList<String> sources = ConfigUtil.getArrayList(configs, General.SOURCE, exception);
		ArrayList<String> sourceAliasNames = ConfigUtil.getArrayList(configs, ExecuteSQL.SOURCE_ALIASE_NAMES, exception);
		String query = ConfigUtil.getString(configs, ExecuteSQL.QUERY);

		if (sourceAliasNames == null || sourceAliasNames.isEmpty()) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, ExecuteSQL.SOURCE_ALIASE_NAMES);
		} else {
			if (sources != null && !sources.isEmpty()) {
				if (sources.size() != sourceAliasNames.size()) {
					exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, General.SOURCE, ExecuteSQL.SOURCE_ALIASE_NAMES);
				}
			}
		}

		if (query == null || query.isEmpty()) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, ExecuteSQL.QUERY);
		}

		configBean.setSourceAliaseNames(sourceAliasNames);
		configBean.setQuery(query);

	}

}

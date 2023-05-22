package com.anvizent.elt.core.spark.mapping.validator;

import java.io.Serializable;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.config.bean.SeekDetails;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.Mapping;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class MappingValidator implements Serializable {
	private static final long serialVersionUID = 1L;

	protected InvalidConfigException exception;
	protected boolean canCreateBean;

	protected void addException(String message, Object... fields) {
		exception.add(message, fields);
		canCreateBean = false;
	}

	public MappingConfigBean validateAndSetBean(InvalidConfigException exception, LinkedHashMap<String, String> configs, SeekDetails seekDetails,
	        Mapping mapping, String configName) throws ImproperValidationException, InvalidConfigException {
		if (ConfigUtil.isAllEmpty(configs, getConfigsList())) {
			return null;
		}

		canCreateBean = true;
		this.exception = exception;

		MappingConfigBean configBean = validateAndSetBean(configs);
		configBean.setComponentName(configs.get(General.NAME));
		configBean.setConfigName(configName);
		configBean.setMappingConfigName(mapping.getMappingName());
		configBean.setSeekDetails(seekDetails);

		if (canCreateBean) {
			return configBean;
		} else {
			return null;
		}
	}

	public abstract MappingConfigBean validateAndSetBean(LinkedHashMap<String, String> configs) throws ImproperValidationException, InvalidConfigException;

	public abstract String[] getConfigsList() throws ImproperValidationException;
}

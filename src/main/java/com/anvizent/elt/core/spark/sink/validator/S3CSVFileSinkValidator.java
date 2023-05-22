package com.anvizent.elt.core.spark.sink.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.S3;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.sink.config.bean.S3FileSinkConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class S3CSVFileSinkValidator extends CSVFileSinkValidator {

	private static final long serialVersionUID = 1L;

	public S3CSVFileSinkValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException {
		S3FileSinkConfigBean configBean = new S3FileSinkConfigBean();

		validateAndSetPathAndOptions(configBean, configs);
		validateAndSetConnectionFields(configBean, configs);

		return configBean;
	}

	protected void validateAndSetConnectionFields(S3FileSinkConfigBean configBean, LinkedHashMap<String, String> configs) {
		validateMandatoryFields(configs, S3.ACCESS_KEY, S3.SECRET_KEY, S3.BUCKET_NAME);
		setConfigBean(configBean, configs);
	}

	private void setConfigBean(S3FileSinkConfigBean configBean, LinkedHashMap<String, String> configs) {
		configBean.setS3Connection(ConfigUtil.getString(configs, S3.ACCESS_KEY), ConfigUtil.getString(configs, S3.SECRET_KEY),
		        ConfigUtil.getString(configs, S3.BUCKET_NAME));
	}
}
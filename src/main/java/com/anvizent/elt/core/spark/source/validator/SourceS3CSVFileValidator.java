package com.anvizent.elt.core.spark.source.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.S3;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.source.config.bean.SourceS3CSVFileConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class SourceS3CSVFileValidator extends SourceCSVFileValidator {

	private static final long serialVersionUID = 1L;

	public SourceS3CSVFileValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {

		SourceS3CSVFileConfigBean configBean = new SourceS3CSVFileConfigBean();

		validateAndSetPath(configBean, configs);

		validateMandatoryFields(configs, S3.ACCESS_KEY, S3.SECRET_KEY, S3.BUCKET_NAME);

		configBean.setPath(Constants.Protocol.S3 + configs.get(S3.BUCKET_NAME) + configBean.getPath(), false);

		validateAndSetSchema(configBean, configs);

		getOptions(configBean, configs);

		configBean.setS3Connection(configs.get(S3.ACCESS_KEY), configs.get(S3.SECRET_KEY), configs.get(S3.BUCKET_NAME));

		return configBean;
	}
}

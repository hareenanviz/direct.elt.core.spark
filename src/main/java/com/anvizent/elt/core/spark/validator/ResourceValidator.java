package com.anvizent.elt.core.spark.validator;

import java.io.File;
import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.store.ResourceConfig;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.Constants.ARGSConstant;
import com.anvizent.elt.core.spark.exception.InvalidConfigurationReaderProperty;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class ResourceValidator implements Serializable {
	private static final long serialVersionUID = 1L;

	protected InvalidConfigException exception;

	public ResourceValidator() {
		this.exception = new InvalidConfigException();
	}

	public void validate(ConfigBean configBean, ResourceConfig resourceConfig)
	        throws InvalidConfigException, UnimplementedException, InvalidConfigurationReaderProperty, RecordProcessingException {
		if (resourceConfig == null) {
			throw new InvalidConfigurationReaderProperty("Argument '" + ARGSConstant.RESOURCE_CONFIG + "' is mandatory for '" + configBean.getName()
			        + "' with name '" + configBean.getConfigName() + "'");
		} else if (StringUtils.isBlank(getMandatoryLocation(resourceConfig))) {
			resourceConfig.getException().add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, getMandatoryLocationProperty());
		} else {
			File file = new File(getMandatoryLocation(resourceConfig));
			if (!file.exists()) {
				resourceConfig.getException().add(ValidationConstant.Message.IO.FILE_LOCATION_DOSE_NOT_EXISTS, getMandatoryLocationProperty());
			} else if (isMandatoryLocationADirectory() && !file.isDirectory()) {
				resourceConfig.getException().add(ValidationConstant.Message.IO.FILE_LOCATION_IS_NOT_A_DIRECTORY, getMandatoryLocationProperty());
			} else if (!isMandatoryLocationADirectory() && !file.isFile()) {
				resourceConfig.getException().add(ValidationConstant.Message.IO.FILE_LOCATION_IS_NOT_A_FILE, getMandatoryLocationProperty());
			}
		}

		if (resourceConfig.getException().getNumberOfExceptions() == 0 || !doNotProceedAfterMandatoryLocationfailed()) {
			validateResourceConfig(configBean, resourceConfig);
		}

		if (resourceConfig.getException().getNumberOfExceptions() > 0) {
			throw resourceConfig.getException();
		}
	}

	public abstract void validateResourceConfig(ConfigBean configBean, ResourceConfig resourceConfig)
	        throws ImproperValidationException, UnimplementedException, RecordProcessingException;

	public abstract String getMandatoryLocation(ResourceConfig resourceConfig);

	public abstract String getMandatoryLocationProperty();

	public abstract boolean isMandatoryLocationADirectory();

	public abstract boolean doNotProceedAfterMandatoryLocationfailed();
}

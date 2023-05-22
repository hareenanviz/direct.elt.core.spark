package com.anvizent.elt.core.spark.source.resource.validator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnectionByTaskId;
import com.anvizent.elt.core.listener.common.store.ResourceConfig;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.source.config.bean.SourceSQLConfigBean;
import com.anvizent.elt.core.spark.source.resource.config.SQLResourceConfig;
import com.anvizent.elt.core.spark.validator.ResourceValidator;

import flexjson.JSONDeserializer;

/**
 * @author Hareen Bejjanki
 *
 */
public class SourceSQLResourceValidator extends ResourceValidator {
	private static final long serialVersionUID = 1L;

	@Override
	public void validateResourceConfig(ConfigBean configBean, ResourceConfig resourceConfig) throws UnimplementedException, RecordProcessingException {
		SourceSQLConfigBean sqlConfigBean = (SourceSQLConfigBean) configBean;

		File configFile = getConfigFile(sqlConfigBean, resourceConfig);
		String fileContent = getFileContent(configFile);
		sqlConfigBean.setResourceConfig(new JSONDeserializer<SQLResourceConfig>().use(null, SQLResourceConfig.class).deserialize(fileContent));
	}

	private String getFileContent(File configFile) throws RecordProcessingException {
		try (BufferedReader reader = new BufferedReader(new FileReader(configFile))) {
			String line;
			StringBuffer content = new StringBuffer();
			while ((line = reader.readLine()) != null) {
				content.append(line).append("\n");
			}

			return content.toString();
		} catch (IOException exception) {
			throw new RecordProcessingException(exception);
		}
	}

	private File getConfigFile(SourceSQLConfigBean sqlConfigBean, ResourceConfig resourceConfig) throws UnimplementedException, RecordProcessingException {
		String version = getVersion(sqlConfigBean);

		File root = new File(resourceConfig.getRdbmsConfigLocation());
		File driverFolder = new File(root, sqlConfigBean.getConnection().getDriver());

		if (!driverFolder.exists()) {
			resourceConfig.getException().add(ValidationConstant.Message.IO.FILE_LOCATION_VALUE_DOSE_NOT_EXISTS, driverFolder.getAbsolutePath());
		} else if (!driverFolder.isDirectory()) {
			resourceConfig.getException().add(ValidationConstant.Message.IO.FILE_LOCATION_VALUE_IS_NOT_A_DIRECTORY, driverFolder.getAbsolutePath());
		}

		File configFile = new File(driverFolder, version + ".json");

		if (!configFile.exists()) {
			File defaultConfigFile = new File(driverFolder, "default.json");
			if (!defaultConfigFile.exists()) {
				resourceConfig.getException().add(ValidationConstant.Message.IO.FILE_LOCATION_VALUE_DOSE_NOT_EXISTS, defaultConfigFile.getAbsolutePath());
			} else if (!defaultConfigFile.isFile()) {
				resourceConfig.getException().add(ValidationConstant.Message.IO.FILE_LOCATION_VALUE_IS_NOT_A_FILE, defaultConfigFile.getAbsolutePath());
			} else {
				resourceConfig.getException().add(ValidationConstant.Message.IO.FILE_LOCATION_VALUE_DOSE_NOT_EXISTS, configFile.getAbsolutePath());
			}

			return defaultConfigFile;
		} else if (!configFile.isFile()) {
			resourceConfig.getException().add(ValidationConstant.Message.IO.FILE_LOCATION_VALUE_IS_NOT_A_FILE, configFile.getAbsolutePath());
		}

		return configFile;
	}

	private String getVersion(SourceSQLConfigBean sqlConfigBean) throws RecordProcessingException {

		Connection connection;
		try {
			connection = (Connection) ApplicationConnectionBean.getInstance().get(new RDBMSConnectionByTaskId(sqlConfigBean.getConnection(), null, -1),
			        true)[0];
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			return databaseMetaData.getDatabaseProductVersion();
		} catch (ImproperValidationException | UnimplementedException | SQLException | TimeoutException exception) {
			throw new RecordProcessingException(exception);
		}
	}

	@Override
	public String getMandatoryLocation(ResourceConfig resourceConfig) {
		return resourceConfig.getRdbmsConfigLocation();
	}

	@Override
	public String getMandatoryLocationProperty() {
		return ConfigConstants.ResourceConfig.RDBMS_CONFIG_LOCATION;
	}

	@Override
	public boolean isMandatoryLocationADirectory() {
		return true;
	}

	@Override
	public boolean doNotProceedAfterMandatoryLocationfailed() {
		return true;
	}

}

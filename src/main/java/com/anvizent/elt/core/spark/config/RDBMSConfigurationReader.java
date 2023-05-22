package com.anvizent.elt.core.spark.config;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.spark.config.util.RDBMSConfigurationReaderUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.exception.ConfigReferenceNotFoundException;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.encryptor.AnvizentEncryptor;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RDBMSConfigurationReader extends ARDBMSConfigurationReader {
	private static final long serialVersionUID = 1L;

	private final ResultSet configsResultSet;
	private final String endPoint;

	public RDBMSConfigurationReader(RDBMSConnection rdbmsConnection, AnvizentEncryptor anvizentEncryptor, String configsSourceQuery, String valuesSourceQuery,
	        String globalValuesSourceQuery, LinkedHashMap<String, DerivedComponentConfiguration> derivedComponents, String endPoint)
	        throws ConfigurationReadingException {
		super(configsSourceQuery, RDBMSConfigurationReaderUtil.getConnection(rdbmsConnection), anvizentEncryptor,
		        ConfigConstants.Reading.RDBMS.VALUES_SOURCE_QUERY_NAME, valuesSourceQuery, ConfigConstants.Reading.RDBMS.GLOBAL_VALUES_SOURCE_QUERY_NAME,
		        globalValuesSourceQuery, derivedComponents);

		try {
			this.endPoint = endPoint;
			Statement configsStatement = connection.createStatement();
			configsResultSet = RDBMSConfigurationReaderUtil.getConfigsStatement(configsSourceQuery, anvizentEncryptor, configsStatement);
			int numberOfColumns = configsResultSet.getMetaData().getColumnCount();

			if (numberOfColumns != 2) {
				throw new ConfigurationReadingException(
				        "Query `" + configsSourceQuery + "` returning `" + numberOfColumns + "` number of columns. Expected 2.");
			}
		} catch (Exception exception) {
			throw new ConfigurationReadingException(exception.getMessage(), exception);
		}
	}

	@Override
	protected ComponentConfiguration getComponentConfiguration()
	        throws ConfigReferenceNotFoundException, InvalidInputForConfigException, ConfigurationReadingException {
		try {
			while (configsResultSet.next()) {
				String configName = RDBMSConfigurationReaderUtil.getConfigKey(configsResultSet, seek);

				RDBMSConfigurationReaderUtil.validateConfigContainsSpace(configName, seek);

				int initialSeek = seek.get();
				LinkedHashMap<String, String> config = RDBMSConfigurationReaderUtil.getConfig(configName, configsResultSet, seek, valuesProperties,
				        globalValuesProperties, endPoint, source);

				return getComponentConfiguration(config, configName, initialSeek, seek.get(), getSeekName(), source, true);
			}
		} catch (SQLException exception) {
			throw new ConfigurationReadingException(exception.getMessage(), exception);
		}

		return null;
	}

	@Override
	public String getSeekName() {
		return ExceptionMessage.AT_RECORD_NUMBER;
	}

	@Override
	protected void afterComponentConfigurationCreated() throws ConfigurationReadingException, ConfigReferenceNotFoundException {
	}
}
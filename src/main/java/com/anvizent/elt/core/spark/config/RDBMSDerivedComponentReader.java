package com.anvizent.elt.core.spark.config;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.SeekDetails;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.spark.config.util.RDBMSConfigurationReaderUtil;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.encryptor.AnvizentEncryptor;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RDBMSDerivedComponentReader extends DerivedComponentReader {

	private static final long serialVersionUID = 1L;

	private final Connection connection;
	private AnvizentEncryptor anvizentEncryptor;
	private Statement configsStatement;
	private ResultSet configsResultSet;

	public RDBMSDerivedComponentReader(RDBMSConnection rdbmsConnection, AnvizentEncryptor anvizentEncryptor) throws ConfigurationReadingException {
		this.anvizentEncryptor = anvizentEncryptor;
		this.connection = RDBMSConfigurationReaderUtil.getConnection(rdbmsConnection);
	}

	@Override
	public DerivedComponentReader reset() {
		configsStatement = null;
		configsResultSet = null;
		seek.set(0);
		return this;
	}

	@Override
	public DerivedComponentReader setConfigSource(String configsSourceQuery) throws ConfigurationReadingException {
		try {
			source = configsSourceQuery;
			configsStatement = connection.createStatement();
			configsResultSet = RDBMSConfigurationReaderUtil.getConfigsStatement(configsSourceQuery, anvizentEncryptor, configsStatement);
			componentName = RDBMSConfigurationReaderUtil.getComponentName(configsResultSet, seek);
		} catch (Exception exception) {
			throw new ConfigurationReadingException(exception.getMessage(), exception);
		}

		return this;
	}

	@Override
	public String getSeekName() {
		return ExceptionMessage.AT_RECORD_NUMBER;
	}

	@Override
	public String getComponentName() {
		return componentName;
	}

	@Override
	public ComponentConfiguration readNextConfiguration() throws ConfigurationReadingException, InvalidInputForConfigException {
		try {
			while (configsResultSet.next()) {
				String configName = RDBMSConfigurationReaderUtil.getConfigKey(configsResultSet, seek);

				RDBMSConfigurationReaderUtil.validateConfigContainsSpace(configName, seek);

				int initialSeek = seek.get();
				LinkedHashMap<String, String> configs = RDBMSConfigurationReaderUtil.getDerivedConfig(configsResultSet, configName, seek);
				ComponentConfiguration componentConfiguration = new ComponentConfiguration(configName,
				        new SeekDetails(initialSeek, seek.get(), getSeekName(), source), configs);

				return componentConfiguration;
			}

		} catch (SQLException exception) {
			throw new ConfigurationReadingException(exception.getMessage(), exception);
		}

		return null;
	}

}

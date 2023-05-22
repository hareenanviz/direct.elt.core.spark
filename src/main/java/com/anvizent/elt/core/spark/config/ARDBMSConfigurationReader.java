package com.anvizent.elt.core.spark.config;

import java.sql.Connection;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.spark.config.util.RDBMSConfigurationReaderUtil;
import com.anvizent.encryptor.AnvizentEncryptor;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public abstract class ARDBMSConfigurationReader extends ConfigurationReader {
	private static final long serialVersionUID = 1L;

	protected final Connection connection;

	public ARDBMSConfigurationReader(String source, Connection connection, AnvizentEncryptor anvizentEncryptor, String valuesSourceQueryName,
			String valuesSourceQuery, String globalValuesSourceQueryName, String globalValuesSourceQuery,
			LinkedHashMap<String, DerivedComponentConfiguration> derivedComponents) throws ConfigurationReadingException {
		super(source, RDBMSConfigurationReaderUtil.getValuesProperties(valuesSourceQueryName, valuesSourceQuery, anvizentEncryptor, connection),
				RDBMSConfigurationReaderUtil.getValuesProperties(globalValuesSourceQueryName, globalValuesSourceQuery, anvizentEncryptor, connection),
				derivedComponents);
		this.connection = connection;
	}
}

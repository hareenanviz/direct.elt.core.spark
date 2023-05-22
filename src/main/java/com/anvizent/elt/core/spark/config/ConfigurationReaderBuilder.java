package com.anvizent.elt.core.spark.config;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Reading;
import com.anvizent.elt.core.spark.constant.Constants.ARGSConstant;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.exception.InvalidConfigurationReaderProperty;
import com.anvizent.encryptor.AnvizentEncryptor;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ConfigurationReaderBuilder {

	private String configsSource;
	private String valuesSource;
	private String globalValuesSource;
	private RDBMSConnection rdbmsConnection;
	private AnvizentEncryptor anvizentEncryptor;
	private LinkedHashMap<String, DerivedComponentConfiguration> derivedComponents;
	private String eoc;

	public ConfigurationReaderBuilder() {

	}

	public String getConfigsSource() {
		return configsSource;
	}

	/**
	 * @param arguments
	 * @throws InvalidConfigurationReaderProperty
	 * @throws UnsupportedEncodingException
	 */
	public ConfigurationReaderBuilder(LinkedHashMap<String, ArrayList<String>> arguments,
	        LinkedHashMap<String, DerivedComponentConfiguration> derivedComponentsReader, String configSource, String eoc)
	        throws InvalidConfigurationReaderProperty, UnsupportedEncodingException {

		setConfigsSource(configSource);
		setValuesSource(arguments.get(ARGSConstant.VALUES).get(0));
		setGlobalValuesSource(arguments.get(ARGSConstant.GLOBAL_VALUES) == null || arguments.get(ARGSConstant.GLOBAL_VALUES).isEmpty() ? null
		        : arguments.get(ARGSConstant.GLOBAL_VALUES).get(0));

		if (arguments.containsKey(ARGSConstant.PRIVATE_KEY)) {
			setEncryptionUtility(arguments.get(ARGSConstant.PRIVATE_KEY).get(0), arguments.get(ARGSConstant.IV).get(0));
		}

		if (arguments.containsKey(ARGSConstant.JDBC_URL)) {
			ArrayList<String> jdbcURLs = arguments.get(ARGSConstant.JDBC_URL);
			ArrayList<String> userNames = arguments.get(ARGSConstant.USER_NAME);
			ArrayList<String> passwords = arguments.get(ARGSConstant.PASSWORD);
			ArrayList<String> drivers = arguments.get(ARGSConstant.DRIVER);

			if (CollectionUtils.isEmpty(userNames)) {
				throw new InvalidConfigurationReaderProperty("MIssing argument: --" + ARGSConstant.USER_NAME);
			}

			if (CollectionUtils.isEmpty(passwords)) {
				throw new InvalidConfigurationReaderProperty("MIssing argument: --" + ARGSConstant.PASSWORD);
			}

			if (CollectionUtils.isEmpty(drivers)) {
				throw new InvalidConfigurationReaderProperty("MIssing argument: --" + ARGSConstant.DRIVER);
			}

			setRDBMSConnection(jdbcURLs.get(0), userNames.get(0), passwords.get(0), drivers.get(0));
		}

		setDerivedComponents(derivedComponentsReader);
		setEndPoint(eoc);
	}

	private ConfigurationReaderBuilder setEndPoint(String eoc) throws InvalidConfigurationReaderProperty {
		if (eoc == null || eoc.isEmpty()) {
			throw new InvalidConfigurationReaderProperty();
		}

		this.eoc = eoc;
		return this;
	}

	public ConfigurationReaderBuilder setConfigsSource(String configsSource) throws InvalidConfigurationReaderProperty {
		if (StringUtils.isBlank(configsSource)) {
			throw new InvalidConfigurationReaderProperty();
		}

		this.configsSource = configsSource;
		return this;
	}

	public ConfigurationReaderBuilder setValuesSource(String valuesSource) throws InvalidConfigurationReaderProperty {
		if (StringUtils.isBlank(valuesSource)) {
			throw new InvalidConfigurationReaderProperty();
		}

		this.valuesSource = valuesSource;
		return this;
	}

	public ConfigurationReaderBuilder setGlobalValuesSource(String globalValuesSource) {
		this.globalValuesSource = globalValuesSource;
		return this;
	}

	public ConfigurationReaderBuilder setRDBMSConnection(RDBMSConnection rdbmsConnection) throws InvalidConfigurationReaderProperty {
		if (rdbmsConnection != null && rdbmsConnection.isNull()) {
			throw new InvalidConfigurationReaderProperty();
		}

		this.rdbmsConnection = rdbmsConnection;
		encryptPassword();
		return this;
	}

	private void encryptPassword() throws InvalidConfigurationReaderProperty {
		if (rdbmsConnection != null && anvizentEncryptor != null) {
			try {
				this.rdbmsConnection.setPassword(anvizentEncryptor.decrypt(rdbmsConnection.getPassword()));
			} catch (Exception exception) {
				throw new InvalidConfigurationReaderProperty("Encryption is set but unable to decrypt password", exception);
			}
		}
	}

	public ConfigurationReaderBuilder setRDBMSConnection(String jdbcUrl, String userName, String password, String driver)
	        throws InvalidConfigurationReaderProperty {
		RDBMSConnection rdbmsConnection = new RDBMSConnection();

		rdbmsConnection.setDriver(driver);
		rdbmsConnection.setJdbcUrl(jdbcUrl);
		rdbmsConnection.setUserName(userName);
		rdbmsConnection.setPassword(password);

		if (rdbmsConnection.isNull()) {
			throw new InvalidConfigurationReaderProperty();
		}

		this.rdbmsConnection = rdbmsConnection;
		encryptPassword();
		return this;
	}

	public ConfigurationReaderBuilder setEncryptionUtility(AnvizentEncryptor anvizentEncryptor) throws InvalidConfigurationReaderProperty {
		this.anvizentEncryptor = anvizentEncryptor;
		encryptPassword();
		return this;
	}

	public ConfigurationReaderBuilder setEncryptionUtility(String privateKey, String iv)
	        throws InvalidConfigurationReaderProperty, UnsupportedEncodingException {
		if (isNull(privateKey, iv)) {
			throw new InvalidConfigurationReaderProperty();
		}

		this.anvizentEncryptor = new AnvizentEncryptor(privateKey, iv);
		return this;
	}

	private boolean isNull(String privateKey, String iv) {
		return privateKey == null || privateKey.isEmpty() || iv == null || iv.isEmpty();
	}

	private ConfigurationReaderBuilder setDerivedComponents(LinkedHashMap<String, DerivedComponentConfiguration> derivedComponentsReader) {
		this.derivedComponents = derivedComponentsReader;
		return this;
	}

	public ConfigurationReader build() throws ConfigurationReadingException {
		validate();

		if (rdbmsConnection != null && !rdbmsConnection.isNull()) {
			return new RDBMSConfigurationReader(rdbmsConnection, anvizentEncryptor, configsSource, valuesSource, globalValuesSource, derivedComponents, eoc);
		} else {
			return new IOConfigurationReader(configsSource, valuesSource, globalValuesSource, derivedComponents, eoc);
		}
	}

	private void validate() throws ConfigurationReadingException {
		if (configsSource == null || configsSource.isEmpty()) {
			throw new ConfigurationReadingException(Reading.CONFIGS_SOURCE + ExceptionMessage.IS_MANDATORY);
		}

		if (valuesSource != null && valuesSource.equals(configsSource)) {
			throw new ConfigurationReadingException(Reading.VALUES_SOURCE + ExceptionMessage.CAN_NOT_BE_SAME_AS + Reading.CONFIGS_SOURCE);
		}

		if (globalValuesSource != null && globalValuesSource.equals(configsSource)) {
			throw new ConfigurationReadingException(Reading.GLOBAL_VALUES_SOURCE + ExceptionMessage.CAN_NOT_BE_SAME_AS + Reading.CONFIGS_SOURCE);
		}

		if (valuesSource != null && globalValuesSource != null && valuesSource.equals(globalValuesSource)) {
			throw new ConfigurationReadingException(Reading.VALUES_SOURCE + ExceptionMessage.CAN_NOT_BE_SAME_AS + Reading.GLOBAL_VALUES_SOURCE);
		}
	}
}

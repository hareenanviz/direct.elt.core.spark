package com.anvizent.elt.core.spark.config;

import java.io.UnsupportedEncodingException;

import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.spark.exception.InvalidConfigurationReaderProperty;
import com.anvizent.encryptor.AnvizentEncryptor;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class DerivedComponentReaderBuilder {

	private RDBMSConnection rdbmsConnection;
	private AnvizentEncryptor anvizentEncryptor;

	public DerivedComponentReaderBuilder setRDBMSConnection(String jdbcUrl, String driver, String userName, String password)
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

	private void encryptPassword() throws InvalidConfigurationReaderProperty {
		if (rdbmsConnection != null && anvizentEncryptor != null) {
			try {
				this.rdbmsConnection.setPassword(anvizentEncryptor.decrypt(rdbmsConnection.getPassword()));
			} catch (Exception exception) {
				throw new InvalidConfigurationReaderProperty("Encryption is set but unable to decrypt password", exception);
			}
		}
	}

	public DerivedComponentReaderBuilder setEncryptionUtility(String privateKey, String iv)
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

	public DerivedComponentReader build() throws ConfigurationReadingException {
		if (rdbmsConnection != null && !rdbmsConnection.isNull()) {
			return new RDBMSDerivedComponentReader(rdbmsConnection, anvizentEncryptor);
		} else {
			return new IODerivedComponentReader();
		}
	}

}

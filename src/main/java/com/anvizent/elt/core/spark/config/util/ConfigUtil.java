package com.anvizent.elt.core.spark.config.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;

import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.constant.Compression;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.KafkaSinkCompression;
import com.anvizent.elt.core.spark.constant.KafkaSinkWriteFormat;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ConfigUtil {

	public static ArrayList<Class<?>> getArrayListOfClass(LinkedHashMap<String, String> config, String key, InvalidConfigException exception)
	        throws ImproperValidationException {
		return getArrayListOfClass(config, key, exception, null);
	}

	private static ArrayList<Class<?>> getArrayListOfClass(LinkedHashMap<String, String> config, String key, InvalidConfigException exception,
	        ArrayList<Class<?>> defaultValue) throws ImproperValidationException {
		if (key == null || config.get(key) == null) {
			return defaultValue;
		} else if (config.get(key).isEmpty()) {
			return new ArrayList<>();
		} else {
			ArrayList<Class<?>> classes = new ArrayList<>();
			CSVRecord csvRecords;
			try {
				csvRecords = getCSVRecord(config.get(key), key);
				String invalidTypes = "";

				for (int i = 0; i < csvRecords.size(); i++) {
					String csvRecord = csvRecords.get(i);
					try {
						if (csvRecord == null || csvRecord.isEmpty()) {
							classes.add(null);
						} else {
							classes.add(Class.forName(csvRecord));
						}
					} catch (ClassNotFoundException e) {
						if (!invalidTypes.isEmpty()) {
							invalidTypes += ", ";
						}

						invalidTypes += csvRecord;
					}
				}

				if (!invalidTypes.isEmpty()) {
					exception.add(ValidationConstant.Message.INVALID_TYPES, invalidTypes, key);
				}
			} catch (InvalidConfigValueException invalidConfigValueException) {
				exception.add(invalidConfigValueException.getMessage(), invalidConfigValueException);
			}

			return classes;
		}
	}

	public static ArrayList<Integer> getArrayListOfIntegers(LinkedHashMap<String, String> config, String key, InvalidConfigException exception)
	        throws ImproperValidationException {
		return getArrayListOfIntegers(config, key, exception, null);
	}

	private static ArrayList<Integer> getArrayListOfIntegers(LinkedHashMap<String, String> config, String key, InvalidConfigException exception,
	        ArrayList<Integer> defaultValue) throws ImproperValidationException {
		if (key == null || config.get(key) == null) {
			return defaultValue;
		} else if (config.get(key).isEmpty()) {
			return new ArrayList<Integer>();
		} else {
			ArrayList<Integer> integers = new ArrayList<Integer>();
			CSVRecord record;
			try {
				record = getCSVRecord(config.get(key), key);
				String invalidIntegers = "";

				for (int i = 0; i < record.size(); i++) {
					String integerAsString = record.get(i);
					try {
						if (integerAsString == null || integerAsString.isEmpty()) {
							integers.add(null);
						} else {
							integers.add(Integer.parseInt(integerAsString));
						}
					} catch (NumberFormatException numberFormatException) {
						if (!invalidIntegers.isEmpty()) {
							invalidIntegers += ", ";
						}

						invalidIntegers += integerAsString;
					}
				}

				if (!invalidIntegers.isEmpty()) {
					exception.add(ValidationConstant.Message.INVALID_INTEGERS, invalidIntegers, key);
				}
			} catch (InvalidConfigValueException invalidConfigValueException) {
				exception.add(invalidConfigValueException.getMessage(), invalidConfigValueException);
			}

			return integers;
		}
	}

	public static ArrayList<String> getArrayList(CSVRecord csvRecord) {
		ArrayList<String> arrayList = new ArrayList<String>();

		for (int i = 0; i < csvRecord.size(); i++) {
			arrayList.add(csvRecord.get(i) == null ? "" : csvRecord.get(i).trim());
		}

		return arrayList;
	}

	private static CSVRecord getCSVRecord(String s, String key) throws ImproperValidationException, InvalidConfigValueException {
		return getCSVRecord(s, ',', key);
	}

	public static CSVRecord getCSVRecord(String s, char delimeter, String key) throws ImproperValidationException, InvalidConfigValueException {
		try {
			CSVFormat format = CSVFormat.RFC4180.withDelimiter(delimeter).withQuote('"');
			CSVParser csvParser = CSVParser.parse(s, format);
			CSVRecord csvRecord = csvParser.getRecords().get(0);
			return csvRecord;
		} catch (NullPointerException exception) {
			throw new ImproperValidationException();
		} catch (IOException | IndexOutOfBoundsException exception) {
			throw new InvalidConfigValueException(key, s);
		}
	}

	private static ArrayList<Class<?>> getArrayListOfClassFromCSVRecord(CSVRecord csvRecord, String key, InvalidConfigException exception)
	        throws ImproperValidationException {
		ArrayList<Class<?>> arrayList = new ArrayList<>();
		String invalidTypes = "";

		for (int i = 0; i < csvRecord.size(); i++) {
			try {
				String value = csvRecord.get(i);
				if (value == null || value.isEmpty()) {
					arrayList.add(null);
				} else {
					arrayList.add(Class.forName(value));
				}
			} catch (ClassNotFoundException classNotFoundException) {
				if (!invalidTypes.isEmpty()) {
					invalidTypes += ", ";
				}

				invalidTypes += csvRecord.get(i);
			}
		}

		if (!invalidTypes.isEmpty()) {
			exception.add(ValidationConstant.Message.INVALID_TYPES, invalidTypes, key);
		}

		return arrayList;
	}

	public static boolean getBoolean(LinkedHashMap<String, String> config, String key, InvalidConfigException exception) {
		return getBoolean(config, key, exception, false);
	}

	public static boolean getBoolean(LinkedHashMap<String, String> config, String key, InvalidConfigException exception, boolean defaultValue) {
		String value = config.get(key);
		if (value != null && !value.isEmpty()) {
			if (value.equalsIgnoreCase(((Boolean) true).toString())) {
				return true;
			} else if (value.equalsIgnoreCase(((Boolean) false).toString())) {
				return false;
			} else {
				exception.add(ValidationConstant.Message.INVALID_BOOLEANS, value, key);
			}

			return Boolean.parseBoolean(value);
		} else {
			return defaultValue;
		}
	}

	public static Compression getCompression(LinkedHashMap<String, String> config, String key, InvalidConfigException exception) {
		return getCompression(config, key, exception, null);
	}

	public static Compression getCompression(LinkedHashMap<String, String> config, String key, InvalidConfigException exception, Compression defaultValue) {
		Compression compression = Compression.getInstance(config.get(key), defaultValue);

		if (compression == null) {
			exception.add(ValidationConstant.Message.INVALID_OR_UNSUPPORTED_COMPRESSIONS, config.get(key), key);
		}

		return compression;
	}

	public static KafkaSinkCompression getKafkaSinkCompression(LinkedHashMap<String, String> config, String key, InvalidConfigException exception,
	        KafkaSinkCompression defaultValue) {
		KafkaSinkCompression compression = KafkaSinkCompression.getInstance(config.get(key), defaultValue);

		if (compression != defaultValue && compression == null) {
			exception.add(ValidationConstant.Message.INVALID_OR_UNSUPPORTED_COMPRESSIONS, config.get(key), key);
		}

		return compression;
	}

	public static KafkaSinkWriteFormat getKafkaSinkWriteFormat(LinkedHashMap<String, String> config, String key, InvalidConfigException exception,
	        KafkaSinkWriteFormat defaultValue) {
		KafkaSinkWriteFormat kafkaSinkWriteFormat = KafkaSinkWriteFormat.getInstance(config.get(key), defaultValue);

		if (kafkaSinkWriteFormat == null) {
			exception.add(ValidationConstant.Message.INVALID_OR_UNSUPPORTED_COMPRESSIONS, config.get(key), key);
		}

		return kafkaSinkWriteFormat;
	}

	// TODO check if usage is funny
	public static boolean isAnyEmpty(LinkedHashMap<String, String> config, String... keys) {
		boolean isAnyEmpty = false;

		for (String key : keys) {
			if (StringUtils.isEmpty(config.get(key))) {
				isAnyEmpty = true;
				break;
			}
		}

		return isAnyEmpty;
	}

	public static boolean isAllEmpty(LinkedHashMap<String, String> config, String... keys) {
		boolean isAllEmpty = true;

		for (String key : keys) {
			if (StringUtils.isNotEmpty(config.get(key))) {
				isAllEmpty = false;
				break;
			}
		}

		return isAllEmpty;
	}

	public static boolean isAllNotEmpty(LinkedHashMap<String, String> config, String... keys) {
		boolean isAllNotEmpty = true;

		for (String key : keys) {
			if (StringUtils.isEmpty(config.get(key))) {
				isAllNotEmpty = false;
				break;
			}
		}

		return isAllNotEmpty;
	}

	public static ArrayList<String> getArrayList(LinkedHashMap<String, String> configs, String key, InvalidConfigException exception)
	        throws ImproperValidationException {
		return getArrayList(configs, key, null, exception);
	}

	private static ArrayList<String> getArrayList(LinkedHashMap<String, String> configs, String key, ArrayList<String> defaultValue,
	        InvalidConfigException exception) throws ImproperValidationException {

		if (StringUtils.isNotEmpty(configs.get(key))) {
			try {
				CSVRecord csvRecord = getCSVRecord(configs.get(key), key);
				return getArrayList(csvRecord);
			} catch (InvalidConfigValueException invalidConfigValueException) {
				exception.add(invalidConfigValueException.getMessage(), invalidConfigValueException);
			}

			return defaultValue;
		} else {
			return defaultValue;
		}

	}

	public static ArrayList<Class<?>> getArrayListOfClassFromCSVRecord(LinkedHashMap<String, String> configs, String key, InvalidConfigException exception)
	        throws ImproperValidationException {
		return getArrayListOfClassFromCSVRecord(configs, key, exception, null);
	}

	private static ArrayList<Class<?>> getArrayListOfClassFromCSVRecord(LinkedHashMap<String, String> configs, String key, InvalidConfigException exception,
	        ArrayList<Class<?>> defaultValue) throws ImproperValidationException {

		if (StringUtils.isNotEmpty(configs.get(key))) {
			try {
				CSVRecord csvRecord = getCSVRecord(configs.get(key), key);
				return getArrayListOfClassFromCSVRecord(csvRecord, key, exception);
			} catch (InvalidConfigValueException invalidConfigValueException) {
				exception.add(invalidConfigValueException.getMessage(), invalidConfigValueException);
			}

			return defaultValue;
		} else {
			return defaultValue;
		}
	}

	public static String getString(LinkedHashMap<String, String> configs, String key) {
		return getString(configs, key, null);
	}

	public static String getString(LinkedHashMap<String, String> configs, String key, String defaultValue) {
		if (key == null || StringUtils.isEmpty(configs.get(key))) {
			return defaultValue;
		} else {
			return configs.get(key);
		}
	}

	public static Integer getInteger(LinkedHashMap<String, String> configs, String key, InvalidConfigException exception) {
		return getInteger(configs, key, exception, null);
	}

	public static Integer getInteger(LinkedHashMap<String, String> configs, String key, InvalidConfigException exception, Integer defaultValue) {
		String value = configs.get(key);

		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else {
			try {
				return Integer.parseInt(value);
			} catch (NumberFormatException numberFormatException) {
				exception.add(ValidationConstant.Message.INVALID_INTEGERS, value, key);
				return null;
			}
		}
	}

	public static Long getLong(LinkedHashMap<String, String> configs, String key, InvalidConfigException exception) {
		return getLong(configs, key, exception, null);
	}

	public static Long getLong(LinkedHashMap<String, String> configs, String key, InvalidConfigException exception, Long defaultValue) {
		String value = configs.get(key);

		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else {
			try {
				return Long.parseLong(value);
			} catch (NumberFormatException numberFormatException) {
				exception.add(ValidationConstant.Message.INVALID_LONGS, value, key);
				return null;
			}
		}
	}

	public static ArrayList<ArrayList<String>> getArrayListOfArrayList(LinkedHashMap<String, String> config, String key, InvalidConfigException exception)
	        throws ImproperValidationException {
		return getArrayListOfArrayList(config, key, exception, null);
	}

	private static ArrayList<ArrayList<String>> getArrayListOfArrayList(LinkedHashMap<String, String> config, String key, InvalidConfigException exception,
	        ArrayList<ArrayList<String>> defaultValue) throws ImproperValidationException {
		if (key == null || config.get(key) == null) {
			return defaultValue;
		} else if (config.get(key).isEmpty()) {
			return new ArrayList<ArrayList<String>>();
		} else {
			ArrayList<ArrayList<String>> values = new ArrayList<>();
			try {
				CSVRecord csvRecords = getCSVRecord(config.get(key), key);
				for (int i = 0; i < csvRecords.size(); i++) {
					ArrayList<String> innerValues = new ArrayList<>();

					if (!csvRecords.get(i).isEmpty()) {
						CSVRecord innerCSVRecords = getCSVRecord(csvRecords.get(i), key);
						for (int j = 0; j < innerCSVRecords.size(); j++) {
							innerValues.add(innerCSVRecords.get(j));
						}
					}

					values.add(innerValues);
				}
			} catch (InvalidConfigValueException invalidConfigValueException) {
				exception.add(invalidConfigValueException.getMessage(), invalidConfigValueException);
			}

			return values;
		}
	}

	public static BigDecimal getBigDecimal(LinkedHashMap<String, String> configs, String key, InvalidConfigException exception) {
		return getBigDecimal(configs, key, exception, null);
	}

	public static BigDecimal getBigDecimal(LinkedHashMap<String, String> configs, String key, InvalidConfigException exception, BigDecimal defaultValue) {
		String value = configs.get(key);

		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else {
			try {
				return new BigDecimal(value);
			} catch (Exception e) {
				exception.add(ValidationConstant.Message.INVALID_INTEGERS, value, key);
				return null;
			}
		}
	}

}
package com.anvizent.elt.core.spark.constant;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class Constants {

	public static final String PART = "part";
	public static final String CRC = "crc";
	public static final String FILE_PATH_SEPARATOR = "/";
	public static final File CURRENT_DIRECTORY = new File(".");

	public static class ARGSConstant {
		public static final String HELP = "help";
		public static final String CONSOLE = "console";
		public static final String HTML = "html";
		public static final ArrayList<String> VALID_HELP_VALUES = new ArrayList<String>() {
			private static final long serialVersionUID = 1L;

			{
				add(CONSOLE);
				add(HTML);
			}
		};
		public static final String HTML_FILE_PATH = "htmlFilePath";
		public static final String REPROCESS = "reProcess";
		public static final String SPARK_MASTER = "sparkMaster";
		public static final String APP_NAME = "appName";
		public static final String CONFIGS = "configs";
		public static final String VALUES = "values";
		public static final String GLOBAL_VALUES = "globalValues";
		public static final String STATS_SETTINGS = "statsSettings";
		public static final String RESOURCE_CONFIG = "resourceConfig";
		public static final String ERROR_HANDLERS = "errorHandlers";
		public static final String JOB_SETTINGS = "jobSettings";
		public static final String JDBC_URL = "jdbcUrl";
		public static final String USER_NAME = "userName";
		public static final String PASSWORD = "password";
		public static final String DRIVER = "driver";
		public static final String PRIVATE_KEY = "privateKey";
		public static final String IV = "iv";
		public static final String DERIVED_COMPONENT_CONFIGS = "derivedComponentConfigs";
	}

	public static class Protocol {
		public static final String S3 = "s3a://";
	}

	public static class ExceptionMessage {
		public static final String UNABLE_TO_UPDATE = "Unable to update.";
		public static final String IS_MANDATORY = " is mandatory";
		public static final String CAN_NOT_BE_SAME_AS = " can not be same as ";
		public static final String DESCRIPTION = "Description";
		public static final String NAME = "Name";
		public static final String MANDATORY = "Mandatory";
		public static final String CANNOT_PROVIDE_DEFAULT_VALUE_FOR_MANDATORY_CONFIG = "Can not provide default value for mandatory config.";
		public static final String IMPROPER_VALIDATION_FROM_DEV_TEAM = "Improper validation from dev team!";
		public static final String AT_LINE_NUMBER = "at line number: ";
		public static final String AT_RECORD_NUMBER = "at record number: ";
		public static final String AT_COMPONENT_NUMBER = "at component number: ";
		public static final String REQUEST_TO_CLOSE_OBJECT = "Requested to close the object of type ''{0}'', which is not a resource or a connection!";
		public static final String MORE_THAN_ONE_ROW_IN_LOOKUP_TABLE = "More than one row in look up table ''{0}'' for key-values ''{1}''.";
		public static final String MORE_THAN_EXPECTED_ROWS_IN_FETCHER_TABLE = "More than ''{0}'' row in look up table ''{1}'' for key-values ''{2}''.";
		public static final String FAILED_ON_ZERO_FETCH = "Failed on zero fetch.";
		public static final String FAILED_TO_INSERT = "Failed to insert.";
		public static final String FIELD_ALIAS_NAMES_ARE_DUPLICATE = "Field alias names are duplicate.";
		public static final String NOT_ALLOWED_FOR = " not allowed for ";
		public static final String CACHE_TYPE = "cache type.";
		public static final String ELASTIC_CACHE_ALLOWED_FOR_CLUSTER_CACHE_TYPE = "'Elastic Cache' allowed only for 'cluster' cache type";
		public static final String EHCACHE_ALLOWED_ONLY_FOR_LOCAL_CACHE_TYPE = "'EhCache' allowed only for 'local' cache type";
		public static final String CONFIG_IS_NOT_PROPERLY_ENDED = "config ''{0}'' is not properly ended or there is a miss match";
		public static final String END_OF_COMPONENT_NOT_FOUND = "EOC(End of Component) not found for ''{0}''";
		public static final String CONFIGURATION_KEY_CANNOT_CONTAIN_SPACE = "Configuration key cannot contain a space";
		public static final String REFERENCE_NOT_FOUND_FOR_CONFIG = "Reference not found for ''{0}'' in ''{1}'' component of the source ''{2}''";
		public static final String INVALID_CONFIGURATION_KEY = "Invalid configuration key ''{0}''";
		public static final String FIRST_CONFIGURATION_SHOULD_BE_SOURCE = "First configuration should be a source.";
		public static final String INVALID_NUMBER_OF_ARGUMENT = "Invalid number of argument.";
		public static final String ERROR_WHILE_PERFORMING_MAX_OPERATION = "Error while performing max operation on:";
		public static final String ERROR_WHILE_PERFORMING_MIN_OPERATION = "Error while performing min operation on:";
		public static final String CANNOT_PERFORM_AVERAGE_ON_VALUE_OF_TYPE = "Cannot perform average on: ''{0}'' of type: ''{1}''";
		public static final String CANNOT_PERFORM_SUM_ON_VALUE_OF_TYPE = "Cannot perform sum on: ''{0}'' of type: ''{1}''";
		public static final String DUPLICATE_COLUMN_ALIASES_FOUND_IN_LHS_SOURCE_AND_RHS_SOURCE = "Duplicate column alias ''{0}'' found in LHS source and RHS source.";
		public static final String DERIVED_COMPONENT_NAME_CANNOT_BE_NULL_OR_EMPTY = "Derived Component name cannot be null or empty.";
		public static final String DERIVED_COMPONENT_NAME_CANNOT_CONTAIN_SPACE = "Derived Component name cannot contain space.";
		public static final String DERIVED_COMPONENT_NAME_CANNOT_CONTAIN_ANY_SPECIAL_CHARACTER = "Derived Component name cannot any special character.";
		public static final String NOT_A_DERIVED_COMPONENT = "Not a Derived Component.";
		public static final String DERIVED_COMPONENT_CANNOT_HAVE_MAPPING = "Derived Component cannot have mapping.";
		public static final String INVALID_STATS_SETTINGS_CONFIG_EXCEPTION_PREFIX = "Invalid configuration of the stats settings";
		public static final String END_OF_MAPPING_NOT_FOUND = "EOM(End of Mapping) not found for ''{0}''";
		public static final String START_OF_MAPPING_NOT_FOUND = "SOM(Start of Mapping) not found for ''{0}''";
		public static final String END_OF_STATS_NOT_FOUND = "EOM(End of Stats) not found for ''{0}''";
		public static final String START_OF_STATS_NOT_FOUND = "SOM(Start of Stats) not found for ''{0}''";
		public static final String END_OF_EH_NOT_FOUND = "EOEH(End of Error Handler) not found for ''{0}''";
		public static final String START_OF_EH_NOT_FOUND = "SOEH(Start of Error Handler) not found for ''{0}''";
		public static final String INVALID_INVALID_TO_SPECIFY_WHEN_VALUE_IS = "''{0}'' is invalid to specify when ''{1}'' is ''{2}''";
		public static final String MANDATORY_WHEN_VALUE_IS = "''{0}'' is mandatory to specify when ''{1}'' is ''{2}''";
		public static final String FIELDS_ARE_NOT_PRESENT_IN_STRUCTURE = "Field(s) ''{0}'' is(are) not present in the structure.";
	}

	public static class General {
		public static final String PLACEHOLDER = "${key}";
		public static final String CONFIG_SEPARATOR = ",";
		public static final String KEY_SEPARATOR = ".";
		public static final String NEW_LINE = "\n";
		public static final String TAB = "\t";
		public static final String DATE_FORMAT = "yyyy-MM-dd";
		public static final String TIME_FORMAT = "HH:ss:mm";
		public static final String SQL_TYPES = "SQL_TYPES";
		public static final String JAVA_TYPES = "JAVA_TYPES";
		public static final String AI_COLUMN = "AI_COLUMN";
		public static final String STRUCTURE = "Structure";
		public static final String COLON = ":";
		public static final String OPEN_PARENTHESIS = "(";
		public static final String CLOSE_PARENTHESIS = ")";
		public static final String OPEN_BRACE = "{";
		public static final String CLOSE_BRACE = "}";
		public static final String PROPERTY_COMMENT = "#";
		public static final String WHERE_FIELDS_PLACEHOLDER_START = "$f{";
		public static final String WHERE_FIELDS_PLACEHOLDER_END = "}";
		public static final char AMP = '&';
		public static final char QUERY_STRING = '?';
		@SuppressWarnings("rawtypes")
		public static final Class DEFAULT_TYPE_FOR_FILTER_BY_REGEX = String.class;
		public static final String DERIVED_COMPONENT = "derivedcomponent";
		public static final String DERIVED_COMPONENT_NAME_REGEX = "[a-zA-Z0-9]{1,}";
		public static final String LOOKUP_CACHED = "CACHED";
		public static final String LOOKUP_RESULTING_ROW = "RESULTING_ROW";
		public static final String LOOKEDUP_ROWS_COUNT = "LOOKEDUP_ROWS_COUNT";
		public static final String RECORD_EXISTS = "RECORD_EXISTS";
		public static final String RECORD_FROM_CACHE = "RECORD_FROM_CACHE";
		public static final String DO_UPDATE = "DO_UPDATE";
		public static final String KEYS = "KEYS";
		public static final String USE_UNICODE = "useUnicode=yes";
		public static final String CHARSET_ENCODING = "characterEncoding=UTF-8";
		public static final String REWRITE_BATCHED_STATEMENTS = "rewriteBatchedStatements=true";
		public static final String NO_PARTITION = "NO_PARTITION";
		public static final String RANGE_PARTITION_TYPE = "RANGE_PARTITION_TYPE";
		public static final String DISTINCT_FIELDS_PARTITION_TYPE = "DISTINCT_FIELDS_PARTITION_TYPE";
		public static final String BATCH_PARTITION_TYPE = "BATCH_PARTITION_TYPE";

		public static class Operator {
			public static final String EQUAL_TO = "=";
			public static final String NOT_EQUAL_TO = "!=";
		}
	}

	public static void main(String[] args) throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy");

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(format.parse("01/01/2021"));

		for (int i = 0; i < 400; i++) {
			Calendar calendar2 = Calendar.getInstance();
			calendar2.setTime(calendar.getTime());
			calendar2.set(Calendar.MONTH, (calendar.get(Calendar.MONTH) / 3 + 1) * 3 - 3);
			calendar2.set(Calendar.DATE, 1);
			System.out.println(calendar2.getTime());
			calendar.add(Calendar.DATE, 1);
		}
	}

	@SuppressWarnings("rawtypes")
	public static class Type {
		public static final ArrayList<Class> DIMENSIONS = new ArrayList<Class>() {
			private static final long serialVersionUID = 1L;

			{
				add(Date.class);
				add(String.class);
				add(Character.class);
				add(Boolean.class);
			}
		};
	}

	public static class SQL {
		public static final String SELECT = "SELECT ";
		public static final String FROM = "FROM ";
		public static final String AND = " AND ";
		public static final String WHERE = "WHERE ";
		public static final String INSERT = "INSERT ";
		public static final String INTO = "INTO ";
		public static final String UPDATE = "UPDATE ";
		public static final String SET = "SET ";
		public static final String BINDING_PARAM = "?";
		public static final String BACK_QUOTE = "`";
		public static final String AS = "AS ";
		public static final String VALUES = " VALUES ";
		public static final String AS_TABLE = "AS table1";
		public static final String COMMA = ", ";
		public static final String LIMIT = "LIMIT";
		public static final String TOP = "TOP";
		public static final String ON_DUPLICATE_UPDATE = "ON DUPLICATE KEY UPDATE";
		public static final String NOT = "NOT(";
		public static final String EQUAL_TO = "<=>";
	}

	public static class NOSQL {
		public static final String RETHINK_DB_ID = "id";
		public static final String ARANGO_DB_KEY = "_key";
		public static final String ARANGO_DB_ID = "_id";
		public static final String ARANGO_DB_REVISION = "_rev";

		public static class RethinkDB {
			public static final String DURABILITY = "durability";
			public static final String SOFT = "soft";
			public static final String CONFLICT = "conflict";

			public static class Response {
				public static final String DELETED = "deleted";
				public static final String FIRST_ERROR = "firstError";
				public static final String INSERTED = "inserted";
				public static final String UNCHANGED = "unchanged";
				public static final String REPLACED = "replaced";
				public static final String ERRORS = "errors";
				public static final String SKIPPED = "skipped";
			}
		}
	}
}

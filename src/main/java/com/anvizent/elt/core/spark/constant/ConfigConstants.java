package com.anvizent.elt.core.spark.constant;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ConfigConstants {

	public static class General {
		public static final String YES = "Yes";
		public static final String NO = "No";
		public static final String SOURCE_STREAM = "source.stream";
		public static final String DEFAULT_STREAM = "DEFAULT_STREAM";
		public static final String NAME = "name";
		public static final String SOURCE = "source";
		public static final String PERSIST = "persist";
		public static final String EOC = "eoc";
		public static final String SOM = "som";
		public static final String EOM = "eom";
		public static final String SOS = "sos";
		public static final String EOS = "eos";
		public static final String EOR = "eor";
		public static final String EORC = "eorc";
		public static final String SOEH = "soeh";
		public static final String EOEH = "eoeh";
		public static final String EH = "eh";
		public static final String SOJS = "sojs";
		public static final String EOJS = "eojs";
		public static final String CONFIG = "config";
		public static final String SINK = "sink";
		public static final String OPERATION = "operation";
		public static final String FILTER = "filter";

		public static final String FIELDS = "fields";
		public static final String FIELD_INDEXES = "field.indexes";
		public static final String TYPES = "types";

		public static final String MAX_RETRY_COUNT = "on.failure.max.retry.count";
		public static final String RETRY_DELAY = "retry.delay";

		public static final int DECIMAL_PRECISION = 10;
		public static final int DECIMAL_SCALE = 5;

		public static final String CONFIG_START_PLACEHOLDER = "${";
		public static final String CONFIG_END_PLACEHOLDER = "}";

		public static final String AVERAGE_SUM_POST_FIX = "_SUM";
		public static final String AVERAGE_COUNT_POST_FIX = "_COUNT";
		public static final Integer DEFAULT_MAX_RETRY_COUNT = 3;
		public static final Long DEFAULT_RETRY_DELAY = 300l;

		public static final String KEY_FIELDS_CASE_SENSITIVE = "key.fields.case.sensitive";

		public static final String DECIMAL_PRECISIONS = "decimal.precisions";
		public static final String DECIMAL_SCALES = "decimal.scales";

		public static final String CACHE_MAX_ELEMENTS_IN_MEMORY = "cache.max.elements.in.memory";

		public static HashMap<String, String> getConfigMap(String config, String name) {
			HashMap<String, String> configAndName = new HashMap<String, String>();

			configAndName.put(General.CONFIG, config);
			configAndName.put(General.NAME, name);

			return configAndName;
		}

		public static class Type {
			public static final String BYTE_AS_STRING = Byte.class.getCanonicalName();
			public static final String SHORT_AS_STRING = Short.class.getCanonicalName();
			public static final String INTEGER_AS_STRING = Integer.class.getCanonicalName();
			public static final String LONG_AS_STRING = Long.class.getCanonicalName();
			public static final String FLOAT_AS_STRING = Float.class.getCanonicalName();
			public static final String DOUBLE_AS_STRING = Double.class.getCanonicalName();
			public static final String BIG_DECIMAL_AS_STRING = BigDecimal.class.getCanonicalName();
			public static final String CHARACTER_AS_STRING = Character.class.getCanonicalName();
			public static final String BOOLEAN_AS_STRING = Boolean.class.getCanonicalName();
			public static final String STRING_AS_STRING = String.class.getCanonicalName();
			public static final String DATE_AS_STRING = Date.class.getCanonicalName();

			public static final String[] ALLOWED_TYPES_AS_STRINGS = { BYTE_AS_STRING, SHORT_AS_STRING, INTEGER_AS_STRING, LONG_AS_STRING, FLOAT_AS_STRING,
			        DOUBLE_AS_STRING, BIG_DECIMAL_AS_STRING, CHARACTER_AS_STRING, BOOLEAN_AS_STRING, STRING_AS_STRING, DATE_AS_STRING };
		}

		public static class S3 {
			public static final String BUCKET_NAME = "bucket";
			public static final String ACCESS_KEY = "aws.access.key.id";
			public static final String SECRET_KEY = "aws.secret.access.key";
		}

		public static class StatsSettings {
			public static final String STATS_SETTINGS = "Stats Settings";
			public static final String STATS_SETTINGS_CONFIG_NAME = "statssettings";
			public static final String END_POINT = "end.point";
			public static final String STATS_CATEGORY = "statsCategory";
			public static final String STATS_TYPE = "stats.type";
			public static final String CONSTANT_NAMES = "literal.constant.fields";
			public static final String CONSTANT_VALUES = "literal.constant.values";
			public static final String CONSTANT_TYPES = "literal.constant.types";
		}

		public static class RDDSpecialNames {
			public static final String PAIR = "Pair";
			public static final String REDUCE = "Reduce";
			public static final String TO_ROW = "To Row";
		}

		public static class RethinkDefault {
			public static final int PORT = 28015;
			public static final String DB_NAME = "test";
			public static final String USER = "admin";
			public static final long TIMEOUT = 2000;
		}

		public static class ArangoDBDefault {
			public static final int PORT = 8529;
			public static final String DB_NAME = "_system";
			public static final String USER = "root";
			public static final int TIMEOUT = 0;
		}

		public static class ErrorHandlers {
			public static final String ERROR_HANDLERS = "Error Handlers";
			public static final String EH_RETHINK_SINK = "ehrethinksink";
			public static final String EH_ARANGO_SINK = "eharangodbsink";
			public static final String EH_SQL_SINK = "ehsqlsink";
			public static final String EH_CONSOLE_SINK = "ehconsolesink";
			public static final String DEFAULT_RESOURCE = "defaultresource";
			public static final String EH_NAME = "eh.name";
		}

		public static class JobSettings {
			public static final String JOB_SETTINGS = "Job Settings";
			public static final String JOB_SETTINGS_CONFIG_NAME = "jobsettings";
			public static final String JOB_DETAILS_ID = "job.details.id";
			public static final String ENGINE_URL = "engine.url";
			public static final String JOB_DETAILS_URL = "job.details.url";
			public static final String EXECUTOR_DETAILS_URL = "executor.details.url";
			public static final String APPLICATION_END_TIME_URL = "application.end.time.url";
			public static final String APPLICATION_ID = "application.id";
			public static final String APP_DB_NAME = "app.db.name";
			public static final String CLIENT_ID = "client.id";
			public static final String HOST_NAME = "host.name";
			public static final String PORT_NUMBER = "port.number";
			public static final String USERNAME = "username";
			public static final String PASSWORD = "password";
			public static final String PRIVATE_KEY = "private.key";
			public static final String IV = "iv";
		}
	}

	public static class Source {
		public static class Components {
			public static final HashMap<String, String> SOURCE_SQL = ConfigConstants.General.getConfigMap("sourcesql", "Source SQL");
			public static final HashMap<String, String> SOURCE_CSV = ConfigConstants.General.getConfigMap("sourcecsv", "Source CSV");
			public static final HashMap<String, String> SOURCE_S3_CSV = ConfigConstants.General.getConfigMap("sources3csv", "Source S3 CSV");
			public static final HashMap<String, String> SOURCE_RETHINKDB = ConfigConstants.General.getConfigMap("sourcerethinkdb", "Source RethinkDB");
			public static final HashMap<String, String> SOURCE_ARANGODB = ConfigConstants.General.getConfigMap("sourcearangodb", "Source ArangoDB");
		}

		public static class General {
			public static final String NUMBER_OF_PARTITIONS = "number.of.partitions";
			public static final String PARTITIONS_SIZE = "partitions.size";
		}

		public static class SourceSQL {
			public static final String TABLE_NAME_OR_QUERY = "table.name.or.query";
			public static final String PARTITION_COLUMN = "options.partition.column";
			public static final String LOWER_BOUND = "options.lower.bound";
			public static final String UPPER_BOUND = "options.upper.bound";
			public static final String NUMBER_OF_PARTITIONS = "options.number.of.partitions";
			public static final String QUERY_CONTAINS_OFFSET = "query.contains.offset";
		}

		public static class SourceCSV {
			public static final String HAS_HEADER = "has.header";
			public static final String INFER_SCHEMA = "infer.schema";
			public static final String IGNORE_LEADING_WHITE_SPACE = "ignore.leading.white.space";
			public static final String IGNORE_TRAILING_WHITE_SPACE = "ignore.trailing.white.space";
			public static final String NULL_VALUE = "null.value";
			public static final String MAX_COLUMNS = "max.columns";
			public static final String MAX_CHARS_PER_COLUMN = "max.chars.per.column";
			public static final String MALFORMED_ROWS = "malformed.rows";
			public static final String MULTI_LINE = "multi.line";
			public static final String QUOTE = "quote";
			public static final String DELIMITER = "delimiter";
		}

		public static final class SourceRethinkDB {
			public static final String SELECT_FIELDS = "select.fields";
			public static final String SELECT_FIELD_TYPES = "select.field.types";
			public static final String PARTITION_COLUMNS = "partition.columns";
			public static final String LOWER_BOUND = "lower.bound";
			public static final String UPPER_BOUND = "upper.bound";
			public static final String NUMBER_OF_PARTITIONS = "number.of.partitions";
			public static final String PARTITION_SIZE = "partition.size";
			public static final String LIMIT = "limit";
		}

		public static final class SourceArangoDB {
			public static final String SELECT_FIELDS = "select.fields";
			public static final String SELECT_FIELD_TYPES = "select.field.types";
			public static final String WHERE_CLAUSE = "where.clause";
			public static final String PARTITION_COLUMNS = "partition.columns";
			public static final String LOWER_BOUND = "lower.bound";
			public static final String UPPER_BOUND = "upper.bound";
			public static final String NUMBER_OF_PARTITIONS = "number.of.partitions";
			public static final String PARTITION_SIZE = "partition.size";
			public static final String LIMIT = "limit";
		}
	}

	public static class Operation {

		public static class Components {
			public static final HashMap<String, String> SQL_LOOKUP = ConfigConstants.General.getConfigMap("sqllookup", "SQL LookUp");
			public static final HashMap<String, String> GROUP_BY = ConfigConstants.General.getConfigMap("groupby", "Group By");
			public static final HashMap<String, String> AGGREGATION = ConfigConstants.General.getConfigMap("aggregation", "Aggregation");
			public static final HashMap<String, String> AVERAGE = ConfigConstants.General.getConfigMap("average", "AVERAGE");
			public static final HashMap<String, String> JOIN = ConfigConstants.General.getConfigMap("join", "Join");
			public static final HashMap<String, String> EXPRESSION = ConfigConstants.General.getConfigMap("expression", "Expression");
			public static final HashMap<String, String> EXECUTE_SQL = ConfigConstants.General.getConfigMap("executesql", "Execute SQL");
			public static final HashMap<String, String> EMPTY = ConfigConstants.General.getConfigMap("empty", "Empty");
			public static final HashMap<String, String> SEQUENCE = ConfigConstants.General.getConfigMap("sequence", "Sequence");
			public static final HashMap<String, String> UNION = ConfigConstants.General.getConfigMap("union", "Union");
			public static final HashMap<String, String> SQL_FETCHER = ConfigConstants.General.getConfigMap("sqlfetcher", "SQL Fetcher");
			public static final HashMap<String, String> REMOVE_DUPLICATES_BY_KEY = ConfigConstants.General.getConfigMap("removeduplicatesbykey",
			        "Remove Duplicates By Key");
			public static final HashMap<String, String> REPARTITION = ConfigConstants.General.getConfigMap("repartition", "Repartition");
			public static final HashMap<String, String> RETHINK_LOOKUP = ConfigConstants.General.getConfigMap("rethinklookup", "RethinkDB Lookup");
			public static final HashMap<String, String> RETHINK_FETCHER = ConfigConstants.General.getConfigMap("rethinkfetcher", "RethinkDB Fetcher");
			public static final HashMap<String, String> MLM_LOOKUP = ConfigConstants.General.getConfigMap("mlmlookup", "MLM Lookup");
			public static final HashMap<String, String> ARANGO_DB_LOOKUP = ConfigConstants.General.getConfigMap("arangodblookup", "ArangoDB Lookup");
			public static final HashMap<String, String> ARANGO_DB_FETCHER = ConfigConstants.General.getConfigMap("arangodbfetcher", "ArangoDB Fetcher");
			public static final HashMap<String, String> RESULT_FETCHER = ConfigConstants.General.getConfigMap("resultfetcher", "Result Fetcher");
		}

		public static class General {
			public static final String EXPRESSIONS = "expressions";
			public static final String ARGUMENT_FIELDS = "argument.fields";
			public static final String ARGUMENT_TYPES = "argument.types";

			public static final String SELECT_COLUMNS = "select.columns";
			public static final String SELECT_FIELD_POSITIONS = "select.field.positions";
			public static final String SELECT_COLUMNS_AS_FIELDS = "select.columns.as.fields";
			public static final String CUSTOM_WHERE = "custom.where";
			public static final String INSERT_VALUES = "insert.values";
			public static final String INSERT_VALUE_BY_FIELDS = "insert.value.by.fields";
			public static final String INSERT_VALUE_BY_FIELD_FORMATS = "insert.value.by.field.formats";
			public static final String ON_ZERO_FETCH = "on.zero.fetch";
			public static final String LIMIT_TO_1 = "limit.to.1";
			public static final String MAX_FETCH_LIMIT = "max.fetch.limit";
			public static final String CACHE_TYPE = "cache.type";
			public static final String CACHE_MODE = "cache.mode";
			public static final String CACHE_TIME_TO_IDLE = "cache.time.to.idle.seconds";
			public static final String WHERE_FIELDS = "where.fields";
			public static final String WHERE_COLUMNS = "where.columns";
		}

		public static class Sequence {
			public static final String INITIAL_VALUES = "initial.values";
		}

		public static class SQLLookUp {
			public static final String ORDER_BY = "order.by";
			public static final String ORDER_BY_TYPES = "order.by.types";
		}

		public static class GroupBy {
			public static final String GROUP_BY_FIELDS = "fields";
			public static final String GROUP_BY_FIELDS_POSITIONS = "field.positions";
			public static final String AGGREGATIONS = "agrregations";
			public static final String AGGREGATION_FIELDS = "aggregation.fields";
			public static final String JOIN_AGGREGATION_DELIMETERS = "join.aggregation.delimeters";
			public static final String AGGREGATION_FIELD_ALIAS_NAMES = "aggregation.field.alias.names";
			public static final String AGGREGATION_FIELDS_POSITIONS = "aggregation.field.positions";
		}

		public static class Join {
			public static final String JOIN_TYPE = "type";
			public static final String JOIN_MODE = "mode";
			public static final String LEFT_HAND_SIDE_FIELDS = "left.hand.side.fields";
			public static final String RIGHT_HAND_SIDE_FIELDS = "right.hand.side.fields";
			public static final String LEFT_HAND_SIDE_FIELD_PREFIX = "left.hand.side.field.prefix";
			public static final String RIGHT_HAND_SIDE_FIELD_PREFIX = "right.hand.side.field.prefix";
			public static final String MAX_ROWS_FOR_BROADCAST = "max.rows.for.broadcast";
			public static final String MAX_SIZE_FOR_BROADCAST = "max.size.for.broadcast";
			public static final String LEFT_HAND_SIDE = "LHS";
			public static final String RIGHT_HAND_SIDE = "RHS";
		}

		public static class ExpressionConstant {
			public static final String ARG = "arg_";
			public static final String REPLACE_$ = "$";
		}

		public static class Expression {
			public static final String EXPRESSIONS_FIELD_NAMES = "expressions.field.names";
			public static final String EXPRESSIONS_FIELD_NAMES_INDEXES = "expressions.field.names.indexes";
			public static final String RETURN_TYPES = "return.types";
		}

		public static class ExecuteSQL {
			public static final String SOURCE_ALIASE_NAMES = "source.alias.names";
			public static final String QUERY = "query";
		}

		public static class Union {
			public static final String STRUCTURE_SOURCE_NAME = "structure.source.name";
		}

		public static class RemoveDuplicatesByKey {
			public static final String KEY_FIELDS = "key.fields";
		}

		public static class Repartition {
			public static final String KEY_FIELDS = "key.fields";
			public static final String NUMBER_OF_PARTITIONS = "number.of.partitions";
		}

		public static class RethinkLookUp {
			public static final String SELECT_FIELD_TYPES = "select.field.types";
			public static final String SELECT_FIELD_DATE_FORMATS = "select.field.date.formats";
			public static final String EQ_FIELDS = "eq.fields";
			public static final String EQ_COLUMNS = "eq.columns";
			public static final String LT_FIELDS = "lt.fields";
			public static final String LT_COLUMNS = "lt.columns";
			public static final String GT_FIELDS = "gt.fields";
			public static final String GT_COLUMNS = "gt.columns";
		}

		public static class ArangoDBLookUp {
			public static final String SELECT_FIELD_TYPES = "select.field.types";
			public static final String SELECT_FIELD_DATE_FORMATS = "select.field.date.formats";
			public static final String ORDER_BY_FIELDS = "order.by.fields";
			public static final String ORDER_BY_TYPE = "order.by.type";
			public static final String WAIT_FOR_SYNC = "wait.for.sync";
		}

		public static class ResultFetcher {
			public static final String CLASS_NAMES = "class.names";
			public static final String METHOD_NAMES = "method.names";
			public static final String VAR_ARGS_INDEXES = "var.args.indexes";
			public static final String METHOD_ARGUMENT_FIELDS = "method.argument.fields";
			public static final String RETURN_FIELDS = "return.fields";
			public static final String RETURN_FIELDS_INDEXES = "return.fields.indexes";
		}
	}

	public static class Filter {
		public static class Components {
			public static final HashMap<String, String> FILTER_BY_REGULAR_EXPRESSION = ConfigConstants.General.getConfigMap("filterbyregex",
			        "Filter By Regular Expression");
			public static final HashMap<String, String> FILTER_BY_EXPRESSION = ConfigConstants.General.getConfigMap("filterbyexpression",
			        "Filter By Java Expression");
			public static final HashMap<String, String> FILTER_BY_RESULT = ConfigConstants.General.getConfigMap("filterbyresult", "Filter By Result");
		}

		public static class General {
			public static final String EMIT_STREAM_NAMES = "emit.stream.names";
		}

		public static class Expression {
			public static final String EXPRESSION_INDEXES = "expression.indexes";
		}

		public static class RegularExpression {
			public static final String FIELDS = "fields";
			public static final String REGULAR_EXPRESSIONS = "regular.expressions";
			public static final String IGNORE_ROW_IF_NULL = "ignore.row.if.null";
		}
	}

	public static class Sink {

		public static class Components {
			public static final HashMap<String, String> JSON_SINK = ConfigConstants.General.getConfigMap("jsonsink", "JSON File Sink");
			public static final HashMap<String, String> S3_JSON_SINK = ConfigConstants.General.getConfigMap("s3jsonsink", "S3 JSON File Sink");
			public static final HashMap<String, String> SQL_SINK = ConfigConstants.General.getConfigMap("sqlsink", "SQL Sink");
			public static final HashMap<String, String> RETHINK_DB_SINK = ConfigConstants.General.getConfigMap("rethinkdbsink", "RethinkDB Sink");
			public static final HashMap<String, String> CONSOLE_SINK = ConfigConstants.General.getConfigMap("consolesink", "Console Sink");
			public static final HashMap<String, String> KAFKA_SINK = ConfigConstants.General.getConfigMap("kafkasink", "Kafka Sink");
			public static final HashMap<String, String> CSV_SINK = ConfigConstants.General.getConfigMap("csvsink", "CSV Sink");
			public static final HashMap<String, String> S3_CSV_SINK = ConfigConstants.General.getConfigMap("s3csvsink", "S3 CSV File Sink");
			public static final HashMap<String, String> ARANGO_DB_SINK = ConfigConstants.General.getConfigMap("arangodbsink", "ArangoDB Sink");
		}

		public static class General {
			public static final String INSERT_MODE = "insert.mode";
			public static final String WRITE_MODE = "write.mode";
			public static final String DELETE_INDICATOR_FIELD = "delete.indicator.field";

			public static final String ALWAYS_UPDATE = "always.update";
			public static final String CHECK_SUM_FIELD = "checksum.field";

			public static final String KEY_FIELDS = "key.fields";
			public static final String KEY_COLUMNS = "key.columns";

			public static final String FIELD_NAMES_DIFFER_TO_COLUMNS = "field.names.differ.to.columns";
			public static final String COLUMN_NAMES_DIFFER_TO_FIELDS = "column.names.differ.to.fields";

			public static final String META_DATA_FIELDS = "meta.data.fields";

			public static final String BATCH_TYPE = "batch.type";
			public static final String BATCH_SIZE = "batch.size";

			public static final String INIT_MAX_RETRY_COUNT = "on.failure.init.max.retry.count";
			public static final String INIT_RETRY_DELAY = "init.retry.delay";

			public static final String DESTROY_MAX_RETRY_COUNT = "on.failure.destroy.max.retry.count";
			public static final String DESTROY_RETRY_DELAY = "destroy.retry.delay";

			public static final String GENERATE_ID = "generate.id";
		}

		public static class SQLSink {
			public static final String KEY_FIELDS_CASE_SENSITIVE = "key.fields.case.sensitive";
			public static final String PREFETCH_BATCH_SIZE = "prefetch.batch.size";
			public static final String DB_CHECK_MODE = "db.check.mode";
			public static final String REMOVE_ONCE_USED = "remove.once.used";

			public static final String CONSTANT_COLUMNS = "constant.columns";
			public static final String CONSTANT_STORE_VALUES = "constant.store.values";
			public static final String CONSTANT_STORE_TYPES = "constant.store.types";
			public static final String CONSTANT_INDEXES = "constant.indexes";

			public static final String INSERT_CONSTANT_COLUMNS = "insert.constant.columns";
			public static final String INSERT_CONSTANT_STORE_VALUES = "insert.constant.store.values";
			public static final String INSERT_CONSTANT_STORE_TYPES = "insert.constant.store.types";
			public static final String INSERT_CONSTANT_INDEXES = "insert.constant.indexes";

			public static final String UPDATE_CONSTANT_COLUMNS = "update.constant.columns";
			public static final String UPDATE_CONSTANT_STORE_VALUES = "update.constant.store.values";
			public static final String UPDATE_CONSTANT_STORE_TYPES = "update.constant.store.types";
			public static final String UPDATE_CONSTANT_INDEXES = "update.constant.indexes";

			public static final String ON_CONNECT_RUN_QUERY = "on.connect.run.query";
			public static final String BEFORE_COMPONENT_RUN_QUERY = "before.component.run.query";
			public static final String AFTER_COMPONENT_SUCCESS_RUN_QUERY = "after.component.success.run.query";
		}

		public static class CSVSink {
			public static final String QUOTE_ALL = "quote.all";
			public static final String ESCAPE_QUOTES = "escape.quotes";
			public static final String DATE_FORMAT = "date.format";
			public static final String TIMESTAMP_FORMAT = "timestamp.format";
		}

		public static class RethinkSink {
			public static final String CONSTANT_FIELDS = "constant.fields";
			public static final String CONSTANT_TYPES = "constant.types";
			public static final String CONSTANT_VALUES = "constant.values";

			public static final String LITERAL_CONSTANT_FIELDS = "literal.constant.fields";
			public static final String LITERAL_CONSTANT_TYPES = "literal.constant.types";
			public static final String LITERAL_CONSTANT_VALUES = "literal.constant.values";
			public static final String LITERAL_CONSTANT_DATE_FORMATS = "literal.constant.date.formats";

			public static final String INSERT_CONSTANT_FIELDS = "insert.constant.fields";
			public static final String INSERT_CONSTANT_TYPES = "insert.constant.types";
			public static final String INSERT_CONSTANT_VALUES = "insert.constant.values";

			public static final String INSERT_LITERAL_CONSTANT_FIELDS = "insert.literal.constant.fields";
			public static final String INSERT_LITERAL_CONSTANT_TYPES = "insert.literal.constant.types";
			public static final String INSERT_LITERAL_CONSTANT_VALUES = "insert.literal.constant.values";
			public static final String INSERT_LITERAL_CONSTANT_DATE_FORMATS = "insert.literal.constant.date.formats";

			public static final String UPDATE_CONSTANT_FIELDS = "update.constant.fields";
			public static final String UPDATE_CONSTANT_TYPES = "update.constant.types";
			public static final String UPDATE_CONSTANT_VALUES = "update.constant.values";

			public static final String UPDATE_LITERAL_CONSTANT_FIELDS = "update.literal.constant.fields";
			public static final String UPDATE_LITERAL_CONSTANT_TYPES = "update.literal.constant.types";
			public static final String UPDATE_LITERAL_CONSTANT_VALUES = "update.literal.constant.values";
			public static final String UPDATE_LITERAL_CONSTANT_DATE_FORMATS = "update.literal.constant.date.formats";

			public static final String SOFT_DURABILITY = "soft.durability";
		}

		public static class ArangoDBSink {
			public static final String CONSTANT_FIELDS = "constant.fields";
			public static final String CONSTANT_TYPES = "constant.types";
			public static final String CONSTANT_VALUES = "constant.values";

			public static final String LITERAL_CONSTANT_FIELDS = "literal.constant.fields";
			public static final String LITERAL_CONSTANT_TYPES = "literal.constant.types";
			public static final String LITERAL_CONSTANT_VALUES = "literal.constant.values";
			public static final String LITERAL_CONSTANT_DATE_FORMATS = "literal.constant.date.formats";

			public static final String INSERT_CONSTANT_FIELDS = "insert.constant.fields";
			public static final String INSERT_CONSTANT_TYPES = "insert.constant.types";
			public static final String INSERT_CONSTANT_VALUES = "insert.constant.values";

			public static final String INSERT_LITERAL_CONSTANT_FIELDS = "insert.literal.constant.fields";
			public static final String INSERT_LITERAL_CONSTANT_TYPES = "insert.literal.constant.types";
			public static final String INSERT_LITERAL_CONSTANT_VALUES = "insert.literal.constant.values";
			public static final String INSERT_LITERAL_CONSTANT_DATE_FORMATS = "insert.literal.constant.date.formats";

			public static final String UPDATE_CONSTANT_FIELDS = "update.constant.fields";
			public static final String UPDATE_CONSTANT_TYPES = "update.constant.types";
			public static final String UPDATE_CONSTANT_VALUES = "update.constant.values";

			public static final String UPDATE_LITERAL_CONSTANT_FIELDS = "update.literal.constant.fields";
			public static final String UPDATE_LITERAL_CONSTANT_TYPES = "update.literal.constant.types";
			public static final String UPDATE_LITERAL_CONSTANT_VALUES = "update.literal.constant.values";
			public static final String UPDATE_LITERAL_CONSTANT_DATE_FORMATS = "update.literal.constant.date.formats";

			public static final String WAIT_FOR_SYNC = "wait.for.sync";
		}

		public static class ConsoleSink {
			public static final String WRITE_ALL = "write.all";
		}
	}

	public static class ResourceConfig {
		public static final String RDBMS_CONFIG_LOCATION = "rdbms.config.location";
		public static final String RESOURCE_CONFIG = "Resource Configs";
		public static final String RESOURCE_CONFIG_CONFIG_NAME = "resourceconfig";
	}

	public static class File {
		public static final String BASE_PATH = "base.path";
		public static final String PATH = "path";
		public static final String LOCAL_BASE_PATH = "local.base.path";

		public static final String ENCODING = "encoding";
		public static final String ESCAPE_CHAR = "escape.char";
		public static final String COMMENT = "comment";
		public static final String DATE_FORMAT = "date.format";
		public static final String TIMESTAMP_FORMAT = "timestamp.format";

		public static final String TIME_ZONE = "time.zone";
		public static final String LINE_SEP = "line.sep";
		public static final String IGNORE_NULL_FIELDS = "ignore.null.fields";
		public static final String CHAR_TO_ESCAPE_QUOTE_ESCAPING = "char.to.escape.quote.escaping";
		public static final String EMPTY_VALUE = "empty.value";

		public static final String DEFAULT_COMMENT = "#";
		public static final String PART_FILES_PREFIX = "part.files.prefix";
		public static final String COMPRESSION = "compression";
		public static final String SAVE_MODE = "save.mode";
		public static final String SINGLE_FILE = "single.file";
		public static final String FILE_FORMAT = "file.format";

	}

	public static class SQLNoSQL {
		public static final String JDBC_URL = "jdbc.url";
		public static final String IS_QUERY = "is.query";
		public static final String QUERY_ALIAS = "query.alias";
		public static final String USER_NAME = "user.name";
		public static final String PASSWORD = "password";
		public static final String DRIVER = "jdbc.driver";
		public static final String TABLE = "table";
		public static final String DB_NAME = "db.name";
		public static final String HOST = "host";
		public static final String PORT_NUMBER = "port.number";
		public static final String TIMEOUT = "timeout";
		public static final String USE_AI_VALUE = "use.ai.value";
		public static final String UPDATE_USING = "update.using";
	}

	public static class MLM {
		public static final String QUERY_URL = "query.url";
		public static final String INITIATE_URL = "initiate.url";
		public static final String ADD_TO_TASK_KILL_LIST_URL = "add.to.task.kill.list.url";
		public static final String USER_ID = "user.id";
		public static final String CLIENT_ID = "client.id";
		public static final String DATA_SOURCE = "data.source";
		public static final String IS_INCREMENTAL = "is.incremental";
		public static final String KAFKA_TOPIC = "kafka.topic";
		public static final String MEASURES_AS_DIMENSIONS_POST_FIX_DEFAULT = "_as_dimension";
		public static final String ANVIZ_COUNT = "_anviz_count";
		public static final String ANVIZ_UUID = "_anviz_uuid";
		public static final String ANVIZ_COPY_OF = "_anviz_copy_of";
		public static final String TIME_FIELD = "_time";
		public static final String DRUID_TIME_FIELD = "timestamp";
		public static final String DEFAULT_DRUID_TIME_FORMAT = "yyyy-MM-dd'T'hh:mm:ss.SSS'Z'";
		public static final Long TIME_FIELD_STEP = 1000l;
		public static final String TASK_COUNT = "task.count";
		public static final Long TASK_COUNT_DEFAULT = 3l;
		public static final Long REPLICAS_DEFAULT = 3l;
		public static final String REPLICAS = "replicas";
		public static final String TASK_DURATION = "task.duration";
		public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
		public static final String PRIVATE_KEY = "private.key";
		public static final String IV = "iv";
		public static final String DRUID_TIME_FORMAT = "druid.time.format";
		public static final String KEY_FIELDS = "key.fields";
		public static final String CLIENT_TOKEN = "X-Auth-Client-Token";
		public static final String USER_TOKEN = "X-Auth-User-Token";
	}

	public static class Messaging {
		public static final String PREFIX = "kafka.";
		public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
		public static final String TOPIC = "topic";
		public static final String TOPIC_FIELD = "topic.field";
		public static final String KEY_FIELD = "key.field";
		public static final String FORMAT = "format";
		public static final String BUFFER_MEMORY = "buffer.memory";
		public static final String COMPRESSION_TYPE = "compression.type";
		public static final String RETRIES = "retries";
		public static final String BATCH_SIZE = "batch.size";
		public static final String CONNECTIONS_MAX_IDLE_MS = "connections.max.idle.ms";
		public static final String LINGER_MS = "linger.ms";
		public static final String MAX_BLOCK_MS = "max.block.ms";
		public static final String REQUEST_TIMEOUT_MS = "request.timeout.ms";
		public static final String RECONNECT_BACKOFF_MAX_MS = "reconnect.backoff.max.ms";
		public static final String RECONNECT_BACKOFF_MS = "reconnect.backoff.ms";
		public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
		public static final String DATE_FORMAT = "date.format";

	}

	public static class Mapping {
		public static class Description {
			public static final String RENAME = "For renaming a mapping field.";
			public static final String COERCE = "Coercion is used for type substitution which occurs when the runtime input data type is a derived type of the one defined in the schema and the user wants to process the input data according to the runtime data type.";
			public static final String RETAIN = "For retaining a given set of fields or to emit some fields from mapping.";
			public static final String REPLICATE = "For replicating any mapping field. You can use this mapping functionality for replicate any number of times.";
			public static final String DUPLICATE = "For duplicate a measure in mapping as a dimension(string) field.";
			public static final String CONSTANT = "For adding constant values to mapping.";
			public static final String CONDITIONAL_REPLACEMENT_CLEANSING = "For performing conditional replacement cleansing.";
			public static final String DATE_AND_TIME_GRANULARITY = "For performing date and time granularity cleansing.";
			public static final String REPOSITION = "For repositioning the given set of fields.";
		}

		public static class General {
			public static final String MAPPING = "mapping";
			public static final String RENAME = "rename";
			public static final String COERCE = "coerce";
			public static final String RETAIN = "retain";
			public static final String REPLICATE = "replicate";
			public static final String DUPLICATE = "duplicate";
			public static final String CONSTANT = "constant";
			public static final String CONDITIONAL_REPLACEMENT_CLEANSING = "conditional.replacement.cleansing";
			public static final String DATE_AND_TIME_GRANULARITY = "date.and.time.granularity";
			public static final String REPOSITION = "reposition";
		}

		public static class Rename {
			public static final String RENAME_FROM = "mapping.rename.from";
			public static final String RENAME_TO = "mapping.rename.to";
		}

		public static class Coerce {
			public static final String COERCE_FIELDS = "mapping.coerce.fields";
			public static final String COERCE_TO_TYPE = "mapping.coerce.to.type";
			public static final String COERCE_TO_FORMAT = "mapping.coerce.to.format";
			public static final String COERCE_DECIMAL_PRECISION = "mapping.coerce.decimal.precisions";
			public static final String COERCE_DECIMAL_SCALE = "mapping.coerce.decimal.scales";
		}

		public static class Replicate {
			public static final String REPLICATE_FIELDS = "mapping.replicate.fields";
			public static final String REPLICATE_TO_FIELDS = "mapping.replicate.to.fields";
			public static final String REPLICATE_POSITIONS = "mapping.replicate.positions";
		}

		public static class Constant {
			public static final String CONSTANT_FIELDS = "mapping.constants.fields";
			public static final String CONSTANT_FIELDS_TYPES = "mapping.constants.fields.types";
			public static final String CONSTANT_FIELDS_FORMATS = "mapping.constants.fields.formats";
			public static final String CONSTANT_FIELDS_VALUES = "mapping.constants.fields.values";
			public static final String CONSTANT_FIELDS_POSITIONS = "mapping.constants.fields.positions";
			public static final String CONSTANT_DECIMAL_PRECISION = "mapping.constants.decimal.precisions";
			public static final String CONSTANT_DECIMAL_SCALE = "mapping.constants.decimal.scales";
		}

		public static class Duplicate {
			public static final String DUPLICATE_MEASURE_AS_STRING = "mapping.duplicate.measure.as.string";
			public static final String DUPLICATE_MEASURE_AS_STRING_PREFIX = "mapping.duplicate.measure.as.string.prefix";
			public static final String DUPLICATE_MEASURE_AS_STRING_SUFFIX = "mapping.duplicate.measure.as.string.suffix";
			public static final String DUPLICATE_MEASURE_AS_STRING_APPEND_AT = "mapping.duplicate.measure.as.string.append.at";
		}

		public static class Retain {
			public static final String RETAIN_FIELDS = "mapping.retain.fields";
			public static final String RETAIN_EMIT = "mapping.retain.emit";
			public static final String RETAIN_AS = "mapping.retain.fields.as";
			public static final String RETAIN_AT = "mapping.retain.fields.at";
		}

		public static class ConditionalReplacementCleansing {

			public static final String CONDITIONAL_REPLACEMENT_CLEANSING = "mapping.cleansing.conditional.replacement";
			public static final String FIELDS = CONDITIONAL_REPLACEMENT_CLEANSING + ".fields";
			public static final String VALIDATION_TYPES = CONDITIONAL_REPLACEMENT_CLEANSING + ".validation.types";
			public static final String REPLACEMENT_VALUES = CONDITIONAL_REPLACEMENT_CLEANSING + ".values";
			public static final String REPLACEMENT_VALUES_BY_FIELDS = REPLACEMENT_VALUES + ".by.fields";
			public static final String DATE_FORMATS = CONDITIONAL_REPLACEMENT_CLEANSING + ".date.formats";

			public static class RangeCleansing {
				public static final String MIN = CONDITIONAL_REPLACEMENT_CLEANSING + ".min";
				public static final String MAX = CONDITIONAL_REPLACEMENT_CLEANSING + ".max";
			}

			public static class EqualsNotEqualsCleansing {
				public static final String EQUALS = CONDITIONAL_REPLACEMENT_CLEANSING + ".equals";
				public static final String NOT_EQUALS = CONDITIONAL_REPLACEMENT_CLEANSING + ".not.equals";
			}

			public static class MatchesNotMatchesRegexCleansing {
				public static final String MATCHES_REGEX = CONDITIONAL_REPLACEMENT_CLEANSING + ".matches.regex";
				public static final String NOT_MATCHES_REGEX = CONDITIONAL_REPLACEMENT_CLEANSING + ".not.matches.regex";
			}

			public static class CustomJavaExpressionCleansing {
				public static final String EXPRESSIONS = CONDITIONAL_REPLACEMENT_CLEANSING + ".expressions";
				public static final String ARGUMENT_FIELDS = CONDITIONAL_REPLACEMENT_CLEANSING + ".argument.fields";
				public static final String ARGUMENT_TYPES = CONDITIONAL_REPLACEMENT_CLEANSING + ".argument.types";
			}
		}

		public static class DateAndTimeGranularity {
			public static final String FIELDS = "mapping.date.and.time.granularity.fields";
			public static final String GRANULARITIES = "mapping.date.and.time.granularities";
			public static final String ALL_DATE_FIELDS = "mapping.date.and.time.granularity.all.date.fields";
		}

		public static class Reposition {
			public static final String FIELDS = "mapping.reposition.fields";
			public static final String POSITIONS = "mapping.reposition.positions";
		}
	}

	public static class ValidationConstant {
		public static class Message {
			public static final String SINGLE_KEY_MANDATORY = "''{0}'' is mandatory.";
			public static final String IS_MANDATORY_WHEN_PRESENT = "''{0}'' is mandatory when ''{1}'' is present.";
			public static final String EITHER_OF_THE_KEY_IS_MANDATORY = "Either ''{0}'' or ''{1}'' is mandatory.";
			public static final String EITHER_BOTH_ARE_MANDATORY_OR_NONE = "Either ''{0}'' and ''{1}'' are mandatory or none of these.";
			public static final String EITHER_OF_THE_THREE_IS_MANDATORY = "Either ''{0}'' or ''{1}'' or ''{2}'' is mandatory.";
			public static final String IS_MANDATORY_FOR_2 = "''{0}'' is mandatory when ''{1}''/''{2}'' is present.";
			public static final String IS_MANDATORY_FOR_VALUE = "''{0}'' is mandatory when ''{1}'' is ''{2}''.";
			public static final String IS_MANDATORY_FOR_VALUE_1_AND_2_OR_3_IS_PRESENT = "''{0}'' is mandatory when ''{1}'' is ''{2}'' AND ''{3}''/''{4}'' is present.";
			public static final String EITHER_OF_IS_MANDATORY_FOR_VALUE = "''{0}'' or ''{1}'' is mandatory when ''{2}'' is ''{3}''.";

			public static final String MUST_BE_BOOLEAN = "''{0}'' value should be a boolean and madatory.";

			public static final String CAN_NOT_BE_EMPTY = "''{0}'' cannot be null or empty.";
			public static final String DEFAULT_ERROR_HANDLER_NOT_FOUND_WITH_NAME = "Default Error Handler ''{0}'' not found.";
			public static final String INVALID_TO_SPECIFY_SOEH_AND_EOEH = "Invalid to specify 'soeh' and 'eoeh' when error handlers file is not provided.";
			public static final String INVALID_TO_SPECIFY_EH_FOR_VALUE_OF = "Invalid to specify error handler when ''{0}'' is ''{1}''.";
			public static final String INVALID_WHEN_OTHER_PRECENT = "''{0}'' is invalid to specify when ''{1}'' is specified.";
			public static final String INVALID_WHEN_OTHER_NOT_PRECENT = "''{0}'' is invalid to specify when ''{1}'' is not specified.";
			public static final String INVALID_WHEN_OTHER_IS = "''{0}'' is invalid to specify when ''{1}'' value is ''{2}''.";
			public static final String INVALID_WHEN_OTHER_IS_NOT = "''{0}'' is invalid to specify when ''{1}'' value is not ''{2}''.";
			public static final String KEY_IS_INVALID = "''{0}'' is invalid.";
			public static final String INVALID_AGGREGATION = "Provided aggregation ''{0}'' is invalid in ''{1}''.";
			public static final String INVALLID_POSITIONS = "Invalid positions: ''{0}'' and ''{1}''.";
			public static final String INVALID_FOR_OTHER = "''{0}'' with value ''{1}'' is invalid for ''{2}'' with value ''{3}''.";
			public static final String INVALID_INDEX_VALUES = "Index values doesn''t match for ''{0}''";
			public static final String INVALID_VALIDATION_TYPE = "Invalid cleansing validation type in ''{0}''.";
			public static final String INVALID_VALUE_FOR = "Invalid value ''{0}'' for ''{1}''.";
			public static final String IS_INVALID_FOR_2 = "''{0}'' is invalid to specify when ''{1}''/''{2}'' is present.";

			public static final String SIZE_SHOULD_MATCH = "Number of {0}''s should match with number of {1}''s.";
			public static final String POSITIONS_SHOULD_BE_UNIQUE = "Positions should be unique: ''{0}'' and ''{1}''.";

			public static final String EITHER_ONE_ARE_MANDATORY_OR_NONE = "Either ''{0}'' or ''{1}'' is mandatory or none of these.";
			public static final String EITHER_FOURE_ARE_MANDATORY_OR_NONE = "Either all of ''{0}'', ''{1}'', ''{2}'', ''{3}'' to be present or none.";
			public static final String EITHER_BOTH_OR_ONE_IS_MANDATORY = "Either ''{0}'' or ''{1}'' are mandatory or both of these.";
			public static final String KEY_IS_PRESENT_IN_OTHER = "''{0}'' is present in ''{1}''.";
			public static final String FIELDS_NOT_FOUND = "''{0}'' fields not found";
			public static final String FIELDS_ALREADY_EXIST = "''{0}'' fields already exist";
			public static final String CAN_NOT_HAVE_COMMON_VALUES = "''{0}'' and ''{1}'' can not have common values";
			public static final String UNSUPPORTED = "Unsupported ''{0}'' for ''{1}''";
			public static final String DUPLICATE_COMPONENT_NAME = "Duplicate component name: ''{0}''";
			public static final String COMPONENT_NOT_FOUND = "Component with name: ''{0}'' not found";
			public static final String NUMBER_OF_SOURCES = "Number of inputs neither match miniumum or maximum input requirements. Provided inputs are: {0}"
			        + "\n\tMinimum required inputs are: {1}";
			public static final String SOURCE_COMPONENT_SINK = "Source component of ''{0}'' is a sink.";
			public static final String UNSUPPORTED_CONFIGS_FOR_SINK_MODE = "''{0}'', ''{1}'' and ''{2}'' are unsupported for ''{3}''.";
			public static final String UNSUPPORTED_CONFIGS_FOR_DB_SINK_MODE = "''{0}'', ''{1}'', ''{2}'' and ''{3}'' are unsupported for ''{4}''.";
			public static final String ONLY_ONE = "Only one ''{0}'' is allowed when ''{1}''";
			public static final String VALUE_ALREADY_PRESENT_IN_ANOTHER_CONFIG = "''{0}'' is already present in ''{1}''";
			public static final String IS_NOT_PRESENT = "''{0}'' is not present in ''{1}''";

			public static final String LIST_CAN_NOT_HAVE_EMPTY = "''{0}'' value(s) cannot be null or empty.";
			public static final String CAN_NOT_BE_EMPTY_2 = "Either ''{0}'' or ''{1}'' value(s) cannot be null or empty.";
			public static final String COUNT_EQUAL_OR_PLUS_1 = "Number of ''{0}'' should be equal to or one more than number of ''{1}''";

			public static final String INVALID_FOR_GLOBAL_STATS_SETTINGS = "''{0}'' is invalid for ''{1}'' in global stats settings.";
			public static final String INVALID_TO_SPECIFY_SOS_AND_EOS = "Invalid to specify 'sos' and 'eos' when stats settings file is not provided.";

			public static final String CONFIG_NOT_APPLICABLE = "Config ''{0}'' is not applicable";
			public static final String RETRY_IS_MANDATORY = "Retry is mandatory";
			public static final String SINGLE_KEY_MANDATORY_WHEN_TABLE_DOESNT_EXISTS = "''{0}'' is mandatory when ''{1}'' target table doesn't exists.";

			public static final String KEY_IS_ALLOWED_ONLY_FOR_SINGLE_OTHER_KEY = "''{0}'' is allowed to specify only for one ''{1}''.";
			public static final String KEY_CANNOT_BE_EQUAL_TO_OTHER_KEY = "''{0}'' cannot be equal to ''{1}''.";
			public static final String KEY_CANNOT_BE_GREATER_THAN_OTHER_KEY = "''{0}'' cannot be greater than ''{1}''.";
			public static final String RESOURCE_NOT_FOUND = "Resource with name: ''{0}'' not found";
			public static final String DEFAULT_RESOURCE_NOT_FOUND = "Default Resource for Error Handlers not found";
			public static final String EH_SINK_RESOURCE_NOT_FOUND = "Error Handler Sink Resource for Error Handlers not found";
			public static final String IS_NOT_INT = "''{0}'' is not present in '{1}'";

			public static final String INVALID_TYPES = "Invalid type(s) ''{0}'' for ''{1}''";
			public static final String INVALID_INTEGERS = "Invalid integer(s) ''{0}'' for ''{1}''";
			public static final String INVALID_BIG_DECIMAL = "Invalid big decimal(s) ''{0}'' for ''{1}''";
			public static final String INVALID_LONGS = "Invalid long(s) ''{0}'' for ''{1}''";
			public static final String INVALID_BOOLEANS = "Invalid boolean(s) ''{0}'' for ''{1}''";
			public static final String INVALID_OR_UNSUPPORTED_COMPRESSIONS = "Invalid or unsupported compression(s) ''{0}'' for ''{1}''";
			public static final String INVALID_OR_UNSUPPORTED_WRITE_FORMAT = "Invalid or unsupported write format(s) ''{0}'' for ''{1}''";
			public static final String CONFLICTING_VALUES_AT = "Configs ''{0}'' and ''{1}'' are having conflicting values at indexe(s) ''{3}''";

			public static class IO {
				public static final String FILE_LOCATION_DOSE_NOT_EXISTS = "File location specified in config ''{0}'' doesn't exists";
				public static final String FILE_LOCATION_VALUE_DOSE_NOT_EXISTS = "File location ''{0}'' doesn't exists";
				public static final String FILE_LOCATION_IS_NOT_A_DIRECTORY = "File location specified in config ''{0}'' is not a directory";
				public static final String FILE_LOCATION_VALUE_IS_NOT_A_DIRECTORY = "File location ''{0}'' is not a directory";
				public static final String FILE_LOCATION_IS_NOT_A_FILE = "File location specified in config ''{0}'' is not a file";
				public static final String FILE_LOCATION_VALUE_IS_NOT_A_FILE = "File location ''{0}'' is not a file";

				private IO() {
				}
			}
		}
	}

	public static class SchemaValidationConstant {
		public static class Message {
			public static final String FIELD_NOT_IN_SCHEMA = "Field ''{0}'' mensioned in the config ''{1}'' is not present in the schema.";
			public static final String FIELD_IN_SCHEMA = "Field ''{0}'' mensioned in the config ''{1}'' is already present in the schema.";
			public static final String TYPE_MISS_MATCH_FOR_OTHER_FIELD = "Field ''{0}'' data type doesn''t match with field ''{1}'' mensioned in the configs ''{2}'', ''{3}''.";
			public static final String TYPE_MISS_MATCH = "Field ''{0}'' data type must be ''{1}''.";
		}
	}

	public static class Reading {
		public class RDBMS {
			public static final String ENCRYPTION_START = "#e{";
			public static final String ENCRYPTION_END = "}";
			public static final String CONFIGS_SOURCE_QUERY_NAME = "configsSourceQueryName";
			public static final String VALUES_SOURCE_QUERY_NAME = "valuesSourceQueryName";
			public static final String GLOBAL_VALUES_SOURCE_QUERY_NAME = "globalValuesSourceQueryName";
		}

		public static final String CONFIGS_SOURCE = "configsSource";
		public static final String VALUES_SOURCE = "valuesSource";
		public static final String GLOBAL_VALUES_SOURCE = "globalValuesSource";
	}
}

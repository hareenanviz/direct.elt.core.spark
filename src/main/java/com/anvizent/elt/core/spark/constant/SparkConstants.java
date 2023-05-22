package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SparkConstants {
	public static class Options {
		public static final String SEP = "sep";
		public static final String ENCODING = "encoding";
		public static final String QUOTE = "quote";
		public static final String QUOTE_ALL = "quoteAll";
		public static final String ESCAPE = "escape";
		public static final String ESCAPE_QUOTES = "escapeQuotes";
		public static final String COMMENT = "comment";
		public static final String HEADER = "header";
		public static final String INFER_SCHEMA = "inferSchema";
		public static final String IGNORE_LEADING_WHITE_SPACE = "ignoreLeadingWhiteSpace";
		public static final String IGNORE_TRAILING_WHITE_SPACE = "ignoreTrailingWhiteSpace";
		public static final String NULL_VALUE = "nullValue";
		public static final String MAX_COLUMNS = "maxColumns";
		public static final String MAX_CHARS_PER_COLUMN = "maxCharsPerColumn";
		public static final String MODE = "mode";
		public static final String MULTI_LINE = "multiLine";
		public static final String DROP_MALFORMED = "DROPMALFORMED";
		public static final String FAIL_FAST = "FAILFAST";

		public static final String FIELDS = "fields";
		public static final String TYPES = "types";

		public static final String TIMESTAMP_FORMAT = "timestampFormat";
		public static final String DATE_FORMAT = "dateFormat";
		public static final String COMPRESSION = "compression";

		public static final String TIME_ZONE = "timeZone";
		public static final String LINE_SEP = "lineSep";
		public static final String IGNORE_NULL_FIELDS = "ignoreNullFields";
		public static final String CHAR_TO_ESCAPE_QUOTE_ESCAPING = "charToEscapeQuoteEscaping";
		public static final String EMPTY_VALUE = "emptyValue";

		public static final String PARTITION_COLUMN = "partitionColumn";
		public static final String LOWER_BOUND = "lowerBound";
		public static final String UPPER_BOUND = "upperBound";
		public static final String NUMBER_OF_PARTITIONS = "numPartitions";
	}

	public static class S3 {
		// for s3n
		// public static final String NATIVE_S3_FILE_SYSTEM =
		// "org.apache.hadoop.fs.s3native.NativeS3FileSystem";
		public static final String NATIVE_S3_FILE_SYSTEM = "org.apache.hadoop.fs.s3a.S3AFileSystem";
	}

	public static class Config {
		// for s3n
		// public static final String S3_AWS_ACCESS_KEY_ID = "fs.s3a.awsAccessKeyId";
		// public static final String S3_AWS_SECRET_ACCESS_KEY =
		// "fs.s3a.awsSecretAccessKey";
		public static final String S3_AWS_ACCESS_KEY_ID = "fs.s3a.access.key";
		public static final String S3_AWS_SECRET_ACCESS_KEY = "fs.s3a.secret.key";
		public static final String S3_IMPL = "fs.s3a.impl";
	}

	public static class SQLNoSQL {
		public static final String USER = "user";
		public static final String PASSWORD = "password";
		public static final String DRIVER = "driver";
	}

	public static class Kafka {
		public static final String TOPIC = "topic";
		public static final String KEY = "key";
		public static final String VALUE = "value";
	}
}

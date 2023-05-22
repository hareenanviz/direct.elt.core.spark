package com.anvizent.elt.core.spark.sink.doc.helper;

import com.anvizent.elt.core.spark.constant.BatchType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.ArangoDBDefault;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink.ArangoDBSink;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.constant.DBWriteMode;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.util.StringUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ArangoDBSinkDocHelper extends DocHelper {

	public ArangoDBSinkDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "Writes the data into give ArangoDB's table.",
				"Special features are batching, constants and metadata. please read bellow for more information" };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(SQLNoSQL.HOST, General.YES, "", new String[] { "ArangoDB host for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.PORT_NUMBER, General.YES, "",
				new String[] { "ArangoDB port for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.DB_NAME, General.NO, ArangoDBDefault.DB_NAME,
				new String[] { "ArangoDB database name where to write data." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.USER_NAME, General.NO, ArangoDBDefault.USER,
				new String[] { "ArangoDB username for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.PASSWORD, General.NO, "<EMPTY_STRING>",
				new String[] { "ArangoDB password for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.TIMEOUT, General.NO, "" + ArangoDBDefault.TIMEOUT, new String[] { "ArangoDB connection timeout." });

		configDescriptionUtil.addConfigDescription(SQLNoSQL.TABLE, General.YES, "", new String[] { "Target table name" });

		configDescriptionUtil.addConfigDescription(Sink.General.INSERT_MODE, General.NO, DBInsertMode.UPSERT.name(),
				new String[] { "Below are the insert modes allowed",
						DBInsertMode.INSERT.name() + ": Except the record not present in the table and insert the record, if already present throws error.",
						DBInsertMode.UPDATE.name() + ": Except the record present in the table and update the record.",
						DBInsertMode.UPSERT.name()
								+ ": Except the record not present in the table and insert the record, otherwise the record in the table update",
						DBInsertMode.INSERT_IF_NOT_EXISTS.name() + ": Insert the record only if not present in the table." },
				true, HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil.addConfigDescription(Sink.General.WRITE_MODE, General.NO, DBWriteMode.APPEND.name(),
				new String[] { "Below are the write modes allowed", DBWriteMode.APPEND.name() + ":It will create a table if not exists, insert the records.",
						DBWriteMode.FAIL.name() + ":If the table already exists throws error, otherwise write the data.",
						DBWriteMode.IGNORE.name() + ": Insert the records only if table doesn't exists, by creating the table.",
						DBWriteMode.OVERWRITE.name() + ":If the table already created it will drop and create a table and insert the records.",
						DBWriteMode.TRUNCATE.name() + ":Removes all existing data and insert the records." },
				true, HTMLTextStyle.ORDERED_LIST);

		configDescriptionUtil.addConfigDescription(Sink.General.KEY_FIELDS,
				General.YES + ": For " + DBInsertMode.UPDATE.name() + ", " + DBInsertMode.UPSERT.name() + " and " + DBInsertMode.INSERT_IF_NOT_EXISTS.name(),
				"", new String[] { "Key fields" }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(Sink.General.KEY_COLUMNS, General.NO, "", new String[] { "Key column" },
				"Number of key columns should be equal number of key fields.", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(Sink.General.FIELD_NAMES_DIFFER_TO_COLUMNS, General.NO, "",
				new String[] { "Source Field names differ to columns" }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(Sink.General.COLUMN_NAMES_DIFFER_TO_FIELDS, General.NO, "",
				new String[] { "Eqivalent Column names differ to fields" },
				"Number of columns differ to fields should be equal to number of fields differ to columns", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(Sink.General.META_DATA_FIELDS, General.NO, "", new String[] { "Metadata fields differ from key fields." },
				"", Type.LIST_OF_STRINGS);

		// constants
		configDescriptionUtil.addConfigDescription(ArangoDBSink.CONSTANT_FIELDS, General.NO, "",
				new String[] { "Sink constant field names. It is supported for all insert modes other than " + DBInsertMode.UPSERT + "." }, "",
				Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(ArangoDBSink.CONSTANT_VALUES, General.YES + ": If " + ArangoDBSink.CONSTANT_FIELDS + " are provided.", "",
				new String[] { "Sink constant field values. It is supported for all insert modes other than " + DBInsertMode.UPSERT + "." },
				"Number of " + ArangoDBSink.CONSTANT_VALUES + " should be equal to number of " + ArangoDBSink.CONSTANT_FIELDS + ".", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(ArangoDBSink.CONSTANT_TYPES, General.YES + ": If " + ArangoDBSink.CONSTANT_FIELDS + " are provided.", "",
				StringUtil.join(new String[] { "Sink constant's Java types. It is supported for all insert modes other than " + DBInsertMode.UPSERT + ".",
						" Allowed types are:" }, General.Type.ALLOWED_TYPES_AS_STRINGS),
				"Number of " + ArangoDBSink.CONSTANT_TYPES + " should be equal to number of " + ArangoDBSink.CONSTANT_FIELDS + ".", Type.LIST_OF_TYPES);

		// literal constants
		configDescriptionUtil.addConfigDescription(ArangoDBSink.LITERAL_CONSTANT_FIELDS, General.NO, "",
				new String[] { "Sink literal constant field names. It is supported for all insert modes other than " + DBInsertMode.UPSERT + "." }, "",
				Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(ArangoDBSink.LITERAL_CONSTANT_VALUES,
				General.YES + ": If " + ArangoDBSink.LITERAL_CONSTANT_FIELDS + " are provided.", "",
				new String[] { "Sink literal values. It is supported for all insert modes other than " + DBInsertMode.UPSERT + "." },
				"Number of " + ArangoDBSink.LITERAL_CONSTANT_VALUES + " should be equal to number of " + ArangoDBSink.LITERAL_CONSTANT_FIELDS + ".",
				Type.LIST_OF_STRINGS);
		configDescriptionUtil
				.addConfigDescription(ArangoDBSink.LITERAL_CONSTANT_TYPES, General.YES + ": If " + ArangoDBSink.LITERAL_CONSTANT_FIELDS + " are provided.", "",
						StringUtil.join(new String[] {
								"Sink literal constant's Java types. It is supported for all insert modes other than " + DBInsertMode.UPSERT + ".",
								" Allowed types are:" }, General.Type.ALLOWED_TYPES_AS_STRINGS),
						"Number of " + ArangoDBSink.LITERAL_CONSTANT_TYPES + " should be equal to number of " + ArangoDBSink.LITERAL_CONSTANT_FIELDS + ".",
						Type.LIST_OF_TYPES);
		configDescriptionUtil.addConfigDescription(ArangoDBSink.LITERAL_CONSTANT_DATE_FORMATS,
				General.YES + ": If " + ArangoDBSink.LITERAL_CONSTANT_FIELDS + " are provided.", "",
				new String[] { "Sink literal constant's date formats. It is supported for all insert modes other than " + DBInsertMode.UPSERT + "." },
				"Number of " + ArangoDBSink.LITERAL_CONSTANT_DATE_FORMATS + " should be equal to number of " + ArangoDBSink.LITERAL_CONSTANT_FIELDS + ".",
				Type.LIST_OF_STRINGS);

		// insert constants
		configDescriptionUtil.addConfigDescription(ArangoDBSink.INSERT_CONSTANT_FIELDS, General.NO, "",
				new String[] { "Sink constant field names. It is supported only for " + DBInsertMode.UPSERT + " insert mode." }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(ArangoDBSink.INSERT_CONSTANT_VALUES,
				General.YES + ": If " + ArangoDBSink.INSERT_CONSTANT_FIELDS + " are provided.", "",
				new String[] { "Sink constant field values. It is supported only for " + DBInsertMode.UPSERT + " insert mode." },
				"Number of " + ArangoDBSink.INSERT_CONSTANT_VALUES + " should be equal to number of " + ArangoDBSink.INSERT_CONSTANT_FIELDS + ".",
				Type.LIST_OF_STRINGS);
		configDescriptionUtil
				.addConfigDescription(ArangoDBSink.INSERT_CONSTANT_TYPES, General.YES + ": If " + ArangoDBSink.INSERT_CONSTANT_FIELDS + " are provided.", "",
						StringUtil.join(new String[] { "Sink constant's Java types. It is supported only for " + DBInsertMode.UPSERT + " insert mode.",
								" Allowed types are:" }, General.Type.ALLOWED_TYPES_AS_STRINGS),
						"Number of " + ArangoDBSink.INSERT_CONSTANT_TYPES + " should be equal to number of " + ArangoDBSink.INSERT_CONSTANT_FIELDS + ".",
						Type.LIST_OF_TYPES);

		// insert literal constants
		configDescriptionUtil.addConfigDescription(ArangoDBSink.INSERT_LITERAL_CONSTANT_FIELDS, General.NO, "",
				new String[] { "Sink literal constant field names. It is supported only for " + DBInsertMode.UPSERT + " insert mode." }, "",
				Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(ArangoDBSink.INSERT_LITERAL_CONSTANT_VALUES,
				General.YES + ": If " + ArangoDBSink.INSERT_LITERAL_CONSTANT_FIELDS + " are provided.", "",
				new String[] { "Sink literal values. It is supported only for " + DBInsertMode.UPSERT + " insert mode." }, "Number of "
						+ ArangoDBSink.INSERT_LITERAL_CONSTANT_VALUES + " should be equal to number of " + ArangoDBSink.INSERT_LITERAL_CONSTANT_FIELDS + ".",
				Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(ArangoDBSink.INSERT_LITERAL_CONSTANT_TYPES,
				General.YES + ": If " + ArangoDBSink.INSERT_LITERAL_CONSTANT_FIELDS + " are provided.", "",
				StringUtil.join(new String[] { "Sink literal constant's Java types. It is supported only for " + DBInsertMode.UPSERT + " insert mode.",
						" Allowed types are:" }, General.Type.ALLOWED_TYPES_AS_STRINGS),
				"Number of " + ArangoDBSink.INSERT_LITERAL_CONSTANT_TYPES + " should be equal to number of " + ArangoDBSink.INSERT_LITERAL_CONSTANT_FIELDS
						+ ".",
				Type.LIST_OF_TYPES);
		configDescriptionUtil.addConfigDescription(ArangoDBSink.INSERT_LITERAL_CONSTANT_DATE_FORMATS,
				General.YES + ": If " + ArangoDBSink.INSERT_LITERAL_CONSTANT_FIELDS + " are provided.", "",
				new String[] { "Sink literal constant's date formats. It is supported only for " + DBInsertMode.UPSERT + " insert mode." },
				"Number of " + ArangoDBSink.INSERT_LITERAL_CONSTANT_DATE_FORMATS + " should be equal to number of "
						+ ArangoDBSink.INSERT_LITERAL_CONSTANT_FIELDS + ".",
				Type.LIST_OF_STRINGS);

		// update constants
		configDescriptionUtil.addConfigDescription(ArangoDBSink.UPDATE_CONSTANT_FIELDS, General.NO, "",
				new String[] { "Sink constant field names. It is supported only for " + DBInsertMode.UPSERT + " insert mode." }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(ArangoDBSink.UPDATE_CONSTANT_VALUES,
				General.YES + ": If " + ArangoDBSink.UPDATE_CONSTANT_FIELDS + " are provided.", "",
				new String[] { "Sink constant field values. It is supported only for " + DBInsertMode.UPSERT + " insert mode." },
				"Number of " + ArangoDBSink.UPDATE_CONSTANT_VALUES + " should be equal to number of " + ArangoDBSink.UPDATE_CONSTANT_FIELDS + ".",
				Type.LIST_OF_STRINGS);
		configDescriptionUtil
				.addConfigDescription(ArangoDBSink.UPDATE_CONSTANT_TYPES, General.YES + ": If " + ArangoDBSink.UPDATE_CONSTANT_FIELDS + " are provided.", "",
						StringUtil.join(new String[] { "Sink constant's Java types. It is supported only for " + DBInsertMode.UPSERT + " insert mode.",
								" Allowed types are:" }, General.Type.ALLOWED_TYPES_AS_STRINGS),
						"Number of " + ArangoDBSink.UPDATE_CONSTANT_TYPES + " should be equal to number of " + ArangoDBSink.UPDATE_CONSTANT_FIELDS + ".",
						Type.LIST_OF_TYPES);

		// update literal constants
		configDescriptionUtil.addConfigDescription(ArangoDBSink.UPDATE_LITERAL_CONSTANT_FIELDS, General.NO, "",
				new String[] { "Sink literal constant field names. It is supported only for " + DBInsertMode.UPSERT + " insert mode." }, "",
				Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(ArangoDBSink.UPDATE_LITERAL_CONSTANT_VALUES,
				General.YES + ": If " + ArangoDBSink.UPDATE_LITERAL_CONSTANT_FIELDS + " are provided.", "",
				new String[] { "Sink literal values. It is supported only for " + DBInsertMode.UPSERT + " insert mode." }, "Number of "
						+ ArangoDBSink.UPDATE_LITERAL_CONSTANT_VALUES + " should be equal to number of " + ArangoDBSink.UPDATE_LITERAL_CONSTANT_FIELDS + ".",
				Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(ArangoDBSink.UPDATE_LITERAL_CONSTANT_TYPES,
				General.YES + ": If " + ArangoDBSink.UPDATE_LITERAL_CONSTANT_FIELDS + " are provided.", "",
				StringUtil.join(new String[] { "Sink literal constant's Java types. It is supported only for " + DBInsertMode.UPSERT + " insert mode.",
						" Allowed types are:" }, General.Type.ALLOWED_TYPES_AS_STRINGS),
				"Number of " + ArangoDBSink.UPDATE_LITERAL_CONSTANT_TYPES + " should be equal to number of " + ArangoDBSink.UPDATE_LITERAL_CONSTANT_FIELDS
						+ ".",
				Type.LIST_OF_TYPES);
		configDescriptionUtil.addConfigDescription(ArangoDBSink.UPDATE_LITERAL_CONSTANT_DATE_FORMATS,
				General.YES + ": If " + ArangoDBSink.UPDATE_LITERAL_CONSTANT_FIELDS + " are provided.", "",
				new String[] { "Sink literal constant's date formats. It is supported only for " + DBInsertMode.UPSERT + " insert mode." },
				"Number of " + ArangoDBSink.UPDATE_LITERAL_CONSTANT_DATE_FORMATS + " should be equal to number of "
						+ ArangoDBSink.UPDATE_LITERAL_CONSTANT_FIELDS + ".",
				Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(Sink.General.BATCH_TYPE, General.NO, BatchType.NONE.name(),
				new String[] { "Below are the allowed batch types:", BatchType.ALL + ":  Batching based on number of records in the partition.",
						BatchType.BATCH_BY_SIZE + ": Batching based on the batch size provided.", BatchType.NONE + ": Record by record processing." },
				"", "String", true, HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil.addConfigDescription(Sink.General.BATCH_SIZE, General.NO, "0", new String[] { "SQL Sink batch size." }, "", "Integer", true,
				HTMLTextStyle.ORDERED_LIST);

		configDescriptionUtil.addConfigDescription(Sink.General.GENERATE_ID, General.NO, "true",
				new String[] { "Will generate id's manually based on key fields." }, Type.BOOLEAN);

		configDescriptionUtil.addConfigDescription(Sink.ArangoDBSink.WAIT_FOR_SYNC, General.NO, "true",
				new String[] { "To make sure data is durable when an insert query returns." }, Type.BOOLEAN);
	}
}
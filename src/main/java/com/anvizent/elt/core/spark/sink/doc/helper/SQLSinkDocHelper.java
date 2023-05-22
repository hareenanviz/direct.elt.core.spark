package com.anvizent.elt.core.spark.sink.doc.helper;

import com.anvizent.elt.core.spark.constant.BatchType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Components;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink.SQLSink;
import com.anvizent.elt.core.spark.constant.DBCheckMode;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.constant.DBWriteMode;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SQLSinkDocHelper extends DocHelper {

	public SQLSinkDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "Writes the data into give RDBMS's table.",
		        "Special feautures are batching, constants and metadata. please read bellow for more information" };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(SQLNoSQL.DRIVER, General.YES, "", new String[] { "JDBC driver name for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.JDBC_URL, General.YES, "",
		        new String[] { "JDBC url for connecting to the given database.", "Example :jdbc:mysql://192.168.0.135:4475/druiduser_1009047" });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.USER_NAME, General.YES, "", new String[] { "JDBC username for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.PASSWORD, General.YES, "", new String[] { "JDBC password for connecting to the given database." });
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

		configDescriptionUtil.addConfigDescription(SQLSink.KEY_FIELDS_CASE_SENSITIVE, General.NO, "false",
		        new String[] { "Consider key fields for case sensitivity." }, "", "Boolean");

		configDescriptionUtil.addConfigDescription(Sink.General.ALWAYS_UPDATE, General.NO, "false",
		        new String[] { "When its false, given records is verified and if found any changes then its updated." }, "", "Boolean");

		configDescriptionUtil.addConfigDescription(Sink.General.CHECK_SUM_FIELD, General.NO, "",
		        new String[] { "A field based on which the record will be updated if its value differs to the original record." }, "", "String");

		configDescriptionUtil.addConfigDescription(SQLSink.CONSTANT_COLUMNS, General.NO, "",
		        new String[] { "Sink constant field names. It is supported for all insert modes other than Upsert." }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(SQLSink.CONSTANT_STORE_VALUES, General.YES + ": If " + SQLSink.CONSTANT_COLUMNS + " are provided.", "",
		        new String[] { "Sink constant field values. It is supported for all insert modes other than Upsert." },
		        "Number of sink constant values should be equal to number of sink constant fields.", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(SQLSink.CONSTANT_STORE_TYPES,
		        General.YES + ": If " + SQLSink.CONSTANT_COLUMNS + " are provided and create table newly. ", "",
		        new String[] { "Sink constant field SQL types. It is supported for all insert modes other than Upsert." },
		        "Number of sink constant types should be equal to number of sink constant fields.", Type.LIST_OF_TYPES);

		configDescriptionUtil.addConfigDescription(SQLSink.INSERT_CONSTANT_COLUMNS, General.NO, "",
		        new String[] { "Sink insert constant field names. It is supported only for Upsert mode." }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(SQLSink.INSERT_CONSTANT_STORE_VALUES,
		        General.YES + ": If " + SQLSink.INSERT_CONSTANT_COLUMNS + " are provided.", "",
		        new String[] { "Sink insert constant field values. It is supported only for Upsert mode." },
		        "Number of sink insert constant values should be equal to number of sink constant fields.", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(SQLSink.INSERT_CONSTANT_STORE_TYPES,
		        General.YES + ": If " + SQLSink.INSERT_CONSTANT_COLUMNS + " are provided and create table newly.", "",
		        new String[] { "Sink insert constant field SQL types. It is supported only for Upsert mode." },
		        "Number of sink insert constant types should be equal to number of sink constant fields.", Type.LIST_OF_TYPES);

		configDescriptionUtil.addConfigDescription(SQLSink.UPDATE_CONSTANT_COLUMNS, General.NO, "",
		        new String[] { "Sink update constant field names. It is supported only for Upsert mode." }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(SQLSink.UPDATE_CONSTANT_STORE_VALUES,
		        General.YES + ": If " + SQLSink.UPDATE_CONSTANT_COLUMNS + " are provided.", "",
		        new String[] { "Sink update constant field values. It is supported only for Upsert mode." },
		        "Number of sink insert constant values should be equal to number of sink constant fields.", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(SQLSink.UPDATE_CONSTANT_STORE_TYPES,
		        General.YES + ": If " + SQLSink.UPDATE_CONSTANT_COLUMNS + " are provided and create table newly.", "",
		        new String[] { "Sink update constant field SQL types. It is supported only for Upsert mode." },
		        "Number of sink insert constant types should be equal to number of sink constant fields.", Type.LIST_OF_TYPES);

		configDescriptionUtil.addConfigDescription(Sink.General.BATCH_TYPE, General.NO, BatchType.NONE.name(),
		        new String[] { "Below are the allowed batch types:", BatchType.ALL + ":  Batching based on number of records in the partition.",
		                BatchType.BATCH_BY_SIZE + ": Batching based on the batch size provided.", BatchType.NONE + ": Record by record processing." },
		        "", "String", true, HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil.addConfigDescription(Sink.General.BATCH_SIZE, General.NO, "0", new String[] { "SQL Sink batch size." }, "", "Integer", true,
		        HTMLTextStyle.ORDERED_LIST);

		configDescriptionUtil.addConfigDescription(Sink.SQLSink.DB_CHECK_MODE, General.NO, DBCheckMode.REGULAR.toString(),
		        new String[] {
		                "When its " + DBCheckMode.DB_QUERY.toString() + ", component will ignore the " + Sink.General.KEY_FIELDS
		                        + " and use target tables keys to decide whether its a insert or update.",
		                "When its " + DBCheckMode.REGULAR.toString() + ", component will check record by record to determine whether its a insert or update. "
		                        + "Please use " + Sink.SQLSink.PREFETCH_BATCH_SIZE + ", " + Sink.SQLSink.REMOVE_ONCE_USED + " and "
		                        + General.CACHE_MAX_ELEMENTS_IN_MEMORY + " for better performance"
		                        + "Use these only when there are no other jobs running which will modify table data",
		                "When its " + DBCheckMode.LOAD_ALL.toString()
		                        + " it will load all sink table into memory and calculate insert or update to be performand on a perticualr record. "
		                        + "Use this mode only when there are no other jobs running which will modify table data" },
		        "", "Boolean");
		configDescriptionUtil.addConfigDescription(Sink.SQLSink.PREFETCH_BATCH_SIZE, General.NO,
		        "null if " + Sink.SQLSink.DB_CHECK_MODE + " is not " + DBCheckMode.DB_QUERY.toString() + ", 1 otherwise",
		        new String[] { "Prefetch is a mechanism to load aditional records(specified number of records) and "
		                + "caching them to reduce the number hits to DB." },
		        "", "Integer", true, HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil.addConfigDescription(Sink.SQLSink.REMOVE_ONCE_USED, General.NO,
		        "null if " + Sink.SQLSink.DB_CHECK_MODE + " is true, true if its false",
		        new String[] { "When the configs are for prefetch, it remove the record from EHCache once it is used.",
		                "It is more effective when there are no/less duplicates in source component.",
		                "See " + Components.REMOVE_DUPLICATES_BY_KEY.get(General.NAME) },
		        "", "Boolean");

		configDescriptionUtil.addConfigDescription(General.CACHE_MAX_ELEMENTS_IN_MEMORY, General.NO, "",
		        new String[] { "For use of preloaded table keys data, to help in speeding up the job."
		                + "This value determines how many rows to store in memory, other will be spilled into disk."
		                + "If this value is not provided there will be no preloaded data." },
		        "", "Integer");
	}
}
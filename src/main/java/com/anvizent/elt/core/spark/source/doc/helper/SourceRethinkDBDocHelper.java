package com.anvizent.elt.core.spark.source.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.RethinkDefault;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Source.SourceArangoDB;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Source.SourceRethinkDB;
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
public class SourceRethinkDBDocHelper extends DocHelper {

	public SourceRethinkDBDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "Reads the data from RethinkDB's table " };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(SQLNoSQL.HOST, General.YES, "", new String[] { "RethinkDB host for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.PORT_NUMBER, General.NO, "" + RethinkDefault.PORT,
		        new String[] { "RethinkDB port for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.DB_NAME, General.NO, RethinkDefault.DB_NAME,
		        new String[] { "RethinkDB database name where to write data." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.USER_NAME, General.NO, RethinkDefault.USER,
		        new String[] { "RethinkDB username for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.PASSWORD, General.NO, "<EMPTY_STRING>",
		        new String[] { "RethinkDB password for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.TIMEOUT, General.NO, "" + RethinkDefault.TIMEOUT, new String[] { "RethinkDB connection timeout." });

		configDescriptionUtil.addConfigDescription(SQLNoSQL.TABLE, General.YES, "", new String[] { "Source table name" });

		configDescriptionUtil.addConfigDescription(SourceRethinkDB.SELECT_FIELDS, General.YES, "", new String[] { "Select fields from RethinkDB's table" }, "",
		        Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(SourceRethinkDB.SELECT_FIELD_TYPES, General.YES, "",
		        StringUtil.join(new String[] { "Select field's equivalent Java types.", " Allowed types are:" }, General.Type.ALLOWED_TYPES_AS_STRINGS),
		        "Number of " + SourceRethinkDB.SELECT_FIELD_TYPES + " should be equal to number of " + SourceRethinkDB.SELECT_FIELDS + ".", Type.LIST_OF_TYPES);

		configDescriptionUtil.addConfigDescription(SourceArangoDB.LIMIT, General.NO, "", new String[] { "Source records selection limit." });

		configDescriptionUtil.addConfigDescription(SourceRethinkDB.PARTITION_COLUMNS, General.NO, "",
		        new String[] { "Partition column(s) from RethinkDB's table" }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(SourceRethinkDB.LOWER_BOUND,
		        General.YES + ": If only one " + SourceRethinkDB.PARTITION_COLUMNS + " is provided", "",
		        new String[] { "Partition column's lower bound value from RethinkDB's table" }, "", "Integer");
		configDescriptionUtil.addConfigDescription(SourceRethinkDB.UPPER_BOUND,
		        General.YES + ": If only one " + SourceRethinkDB.PARTITION_COLUMNS + " is provided", "",
		        new String[] { "Partition column's upper bound value from RethinkDB's table" }, "", "Integer");
		configDescriptionUtil.addConfigDescription(SourceRethinkDB.NUMBER_OF_PARTITIONS,
		        General.YES + ": If " + SourceRethinkDB.PARTITION_COLUMNS + " are provided", "", new String[] { "Total number of partitions" }, "", "Integer");

		configDescriptionUtil.addConfigDescription(SourceRethinkDB.PARTITION_SIZE, General.NO, "",
		        new String[] { "Partition size to perform batch operation. Does not work if partition columns are provided." }, "", "Integer");
	}

}

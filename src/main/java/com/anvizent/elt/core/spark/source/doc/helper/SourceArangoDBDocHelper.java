package com.anvizent.elt.core.spark.source.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.ArangoDBDefault;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Source.SourceArangoDB;
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
public class SourceArangoDBDocHelper extends DocHelper {

	public SourceArangoDBDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "Reads the data from ArangoDB's table." };
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

		configDescriptionUtil.addConfigDescription(SQLNoSQL.TABLE, General.YES, "", new String[] { "Source table name" });

		configDescriptionUtil.addConfigDescription(SourceArangoDB.SELECT_FIELDS, General.YES, "", new String[] { "Select fields from ArangoDB's table" }, "",
		        Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(SourceArangoDB.SELECT_FIELD_TYPES, General.YES, "",
		        StringUtil.join(new String[] { "Select field's equivalent Java types.", " Allowed types are:" }, General.Type.ALLOWED_TYPES_AS_STRINGS),
		        "Number of " + SourceArangoDB.SELECT_FIELD_TYPES + " should be equal to number of " + SourceArangoDB.SELECT_FIELDS + ".", Type.LIST_OF_TYPES);

		configDescriptionUtil.addConfigDescription(SourceArangoDB.WHERE_CLAUSE, General.NO, "",
		        new String[] { "Where clause for filtering data in source table." });

		configDescriptionUtil.addConfigDescription(SourceArangoDB.LIMIT, General.NO, "", new String[] { "Source records selection limit." });

		configDescriptionUtil.addConfigDescription(SourceArangoDB.PARTITION_COLUMNS, General.NO, "",
		        new String[] { "Partition column(s) from ArangoDB's table" }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(SourceArangoDB.LOWER_BOUND,
		        General.YES + ": If only one " + SourceArangoDB.PARTITION_COLUMNS + " is provided", "",
		        new String[] { "Partition column's lower bound value from ArangoDB's table" }, "", "Integer");
		configDescriptionUtil.addConfigDescription(SourceArangoDB.UPPER_BOUND,
		        General.YES + ": If only one " + SourceArangoDB.PARTITION_COLUMNS + " is provided", "",
		        new String[] { "Partition column's upper bound value from ArangoDB's table" }, "", "Integer");
		configDescriptionUtil.addConfigDescription(SourceArangoDB.NUMBER_OF_PARTITIONS,
		        General.YES + ": If " + SourceArangoDB.PARTITION_COLUMNS + " are provided", "", new String[] { "Total number of partitions" }, "", "Integer");

		configDescriptionUtil.addConfigDescription(SourceArangoDB.PARTITION_SIZE, General.NO, "",
		        new String[] { "Partition size to perform batch operation. Does not work if partition columns are provided." }, "", "Integer");
	}

}

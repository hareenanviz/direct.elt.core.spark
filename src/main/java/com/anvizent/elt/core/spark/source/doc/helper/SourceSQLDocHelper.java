package com.anvizent.elt.core.spark.source.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Source.SourceSQL;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SourceSQLDocHelper extends DocHelper {

	public SourceSQLDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "JDBC url to fetch the records from the given table or a custom query," + " for custom query use 'is.query = true'. " };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(SQLNoSQL.DRIVER, General.YES, "", new String[] { "JDBC driver name for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.JDBC_URL, General.YES, "",
				new String[] { "JDBC url for connecting to the given database.", "Example: jdbc:mysql://192.168.0.130:4475/druiduser_1009047" });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.USER_NAME, General.YES, "", new String[] { "JDBC username for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.PASSWORD, General.YES, "", new String[] { "JDBC password for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SourceSQL.TABLE_NAME_OR_QUERY, General.YES, "",
				new String[] { "To fetch the records from the given table or a custom query." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.IS_QUERY, General.NO, "false",
				new String[] { "A flag indicating a custom query or a table name, for custom query use '" + SQLNoSQL.IS_QUERY + " = true'." }, "", "Boolean");
		configDescriptionUtil.addConfigDescription(SourceSQL.PARTITION_COLUMN,
				"If " + SourceSQL.LOWER_BOUND + ", " + SourceSQL.UPPER_BOUND + ", " + SourceSQL.NUMBER_OF_PARTITIONS + " are specified", "",
				new String[] { "Partition for numberic the data for the specified column this is use for parallelism." });
		configDescriptionUtil.addConfigDescription(SourceSQL.LOWER_BOUND,
				"If " + SourceSQL.PARTITION_COLUMN + ", " + SourceSQL.UPPER_BOUND + ", " + SourceSQL.NUMBER_OF_PARTITIONS + " are specified", "",
				new String[] { "Lower bound for the partition column" }, "", "Integer");
		configDescriptionUtil.addConfigDescription(SourceSQL.UPPER_BOUND,
				"If " + SourceSQL.PARTITION_COLUMN + ", " + SourceSQL.LOWER_BOUND + ", " + SourceSQL.NUMBER_OF_PARTITIONS + " are specified", "",
				new String[] { "Upper bound for the partition column." }, "", "Integer");
		configDescriptionUtil.addConfigDescription(SourceSQL.NUMBER_OF_PARTITIONS,
				"If " + SourceSQL.PARTITION_COLUMN + ", " + SourceSQL.LOWER_BOUND + ", " + SourceSQL.UPPER_BOUND + " are specified", "",
				new String[] { "Number of partition." }, "", "Integer");

	}
}

package com.anvizent.elt.core.spark.operation.doc.helper;

import com.anvizent.elt.core.spark.constant.CacheMode;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.SQLLookUp;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink.SQLSink;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public abstract class SQLRetrievalDocHelper extends DocHelper {

	public SQLRetrievalDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(SQLNoSQL.DRIVER, General.YES, "", new String[] { "JDBC driver name for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.JDBC_URL, General.YES, "",
		        new String[] { "JDBC url for connecting to the given database.", "Example :jdbc:mysql://192.168.0.130:4475/druiduser_1009047" });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.USER_NAME, General.YES, "", new String[] { "JDBC username for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.PASSWORD, General.YES, "", new String[] { "JDBC password for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.TABLE, General.YES, "", new String[] { "The lookup table name." });

		configDescriptionUtil.addConfigDescription(Operation.General.SELECT_COLUMNS, General.YES, "",
		        new String[] { "The select fields to fetch from lookup table." }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(Operation.General.SELECT_COLUMNS_AS_FIELDS, General.NO, "", new String[] { "Alias for the select field" },
		        "Number of aliases should match number of fields", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(Operation.General.SELECT_FIELD_POSITIONS, General.NO, "",
		        new String[] { "The position of the selected field in th mapping." }, "Number of positions should match number of fields",
		        Type.LIST_OF_INTEGERS);

		configDescriptionUtil.addConfigDescription(Operation.General.WHERE_FIELDS,
		        General.YES + ":Either " + Operation.General.WHERE_FIELDS + " or " + Operation.General.CUSTOM_WHERE + " is mandatory", "",
		        new String[] { "The fields using which we have to fetch the row from lookup table." }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(Operation.General.WHERE_COLUMNS, General.NO, "",
		        new String[] { "The columns differ to fields in the lookup table which are to be matched with where fields." },
		        "Number of where columns should be equal to number of where fields.", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(Operation.General.CUSTOM_WHERE,
		        General.YES + ":Either " + Operation.General.WHERE_FIELDS + " or " + Operation.General.CUSTOM_WHERE + " is mandatory", "",
		        new String[] { "Custom where conditions." }, "", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(Operation.SQLLookUp.ORDER_BY, General.NO, "", new String[] { "Order By fields" }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(SQLLookUp.ORDER_BY_TYPES, General.NO, "ASC", new String[] { "Order By types (ASC/DESC)" }, "",
		        Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(SQLSink.KEY_FIELDS_CASE_SENSITIVE, General.NO, "false",
		        new String[] { "Consider key fields for case sensitivity." }, "", "Boolean");

		configDescriptionUtil.addConfigDescription(Operation.General.CACHE_TYPE, General.NO, "",
		        new String[] { "Cache mechanism to follow below are the allowed caching mechanism.", CacheType.NONE.name() + ":It will not cache lookup rows",
		                CacheType.EHCACHE.name() + ":It will use the ehcache for caching mechanism",
		                CacheType.ELASTIC_CACHE.name() + ":It will use the elasticache for caching mechanism",
		                CacheType.MEMCACHE.name() + ":It will use memcache for caching mechanism." },
		        true, HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil.addConfigDescription(Operation.General.CACHE_MODE, General.NO, "",
		        new String[] { "Below are the allowed caching mode.",
		                CacheMode.LOCAL.name() + ":Allowed for only the ehcaching mechanism where it cache the looked up rows in the include slave.",
		                CacheMode.CLUSTER.name() + ":Allowed for ehcaching and elasticache where cache store in respective cluster." },
		        true, HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil.addConfigDescription(General.CACHE_MAX_ELEMENTS_IN_MEMORY, General.YES + ": For Ehcache mechanism", "",
		        new String[] { "Maximum number of records that can be stored in the cache." }, "", "Integer");
		configDescriptionUtil.addConfigDescription(Operation.General.CACHE_TIME_TO_IDLE, General.YES + ": For Ehcache mechanism", "",
		        new String[] { "Let's say that timeToIdleSeconds = 3. Object will be invalidated if it hasn't been requested for more than 3 seconds." }, "",
		        "Integer");

		configDescriptionUtil.addConfigDescription(Operation.General.ON_ZERO_FETCH, General.NO, "",
		        new String[] { "Below are the allowed on zero fetch options.", OnZeroFetchOperation.FAIL + ":It will fail the job on zero fetch.",
		                OnZeroFetchOperation.IGNORE + ":It will return the incoming data set where selected fields will be null.",
		                OnZeroFetchOperation.INSERT + ":It will insert the provided insert values." },
		        true, HTMLTextStyle.ORDERED_LIST);

		configDescriptionUtil.addConfigDescription(Operation.General.INSERT_VALUES,
		        General.YES + ": When " + Operation.General.ON_ZERO_FETCH + " is '" + OnZeroFetchOperation.INSERT + "' AND '"
		                + Operation.General.INSERT_VALUE_BY_FIELDS + "' is not present",
		        "", new String[] { "Insert constants for the select fields" },
		        "Number of insert values and insert fields in total should match number of select fields", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(Operation.General.INSERT_VALUE_BY_FIELDS,
		        General.YES + ": When " + Operation.General.ON_ZERO_FETCH + " is '" + OnZeroFetchOperation.INSERT + "' AND '" + Operation.General.INSERT_VALUES
		                + "' is not present",
		        "", new String[] { "Insert values for the select fields using other fields" },
		        "Number of insert values and insert fields in total should match number of select fields", Type.LIST_OF_STRINGS);

		addSQLRetrievalConfigDescription();
	}

	protected abstract void addSQLRetrievalConfigDescription() throws InvalidParameter;

}

package com.anvizent.elt.core.spark.operation.doc.helper;

import com.anvizent.elt.core.spark.constant.CacheMode;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.RethinkDefault;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.RethinkLookUp;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.util.StringUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public abstract class RethinkRetrievalDocHelper extends DocHelper {

	public RethinkRetrievalDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(SQLNoSQL.HOST, General.YES, "",
		        new String[] { "Host name or ip address for connecting to the given database.", "Example :192.168.0.130" });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.PORT_NUMBER, General.NO, "", new String[] { "Port Number for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.USER_NAME, General.NO, "", new String[] { "Username for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.PASSWORD, General.NO, "", new String[] { "Password for connecting to the given database." });

		configDescriptionUtil.addConfigDescription(SQLNoSQL.DB_NAME, General.NO, "",
		        new String[] { "Schema Name for connecting to the particular Schema in the database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.TIMEOUT, General.NO, "" + RethinkDefault.TIMEOUT, new String[] { "RethinkDB connection timeout." });

		configDescriptionUtil.addConfigDescription(SQLNoSQL.TABLE, General.YES, "", new String[] { "The lookup table name." });

		configDescriptionUtil.addConfigDescription(Operation.General.SELECT_COLUMNS, General.YES, "",
		        new String[] { "The select fields to fetch from lookup table." }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(RethinkLookUp.SELECT_FIELD_TYPES, General.NO, "",
		        StringUtil.join(new String[] { "The select field's eqvivalent java data types." }, General.Type.ALLOWED_TYPES_AS_STRINGS),
		        "Number of " + RethinkLookUp.SELECT_FIELD_TYPES + " provided should be equals to number of " + Operation.General.SELECT_COLUMNS + ".",
		        Type.LIST_OF_TYPES, true, HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil.addConfigDescription(RethinkLookUp.SELECT_FIELD_DATE_FORMATS, General.NO, "",
		        new String[] { "To convert selected date fields in particular date format." }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(Operation.General.SELECT_COLUMNS_AS_FIELDS, General.NO, "", new String[] { "Aliases for the select field" },
		        "Number of " + Operation.General.SELECT_COLUMNS_AS_FIELDS + " should match number of " + Operation.General.SELECT_COLUMNS + ".",
		        Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(Operation.General.SELECT_FIELD_POSITIONS, General.NO, "",
		        new String[] { "The position of the selected field in th mapping." },
		        "Number of " + Operation.General.SELECT_FIELD_POSITIONS + " should match number of " + Operation.General.SELECT_COLUMNS + ".",
		        Type.LIST_OF_INTEGERS);

		configDescriptionUtil.addConfigDescription(RethinkLookUp.EQ_FIELDS,
		        General.NO + ": Either '" + RethinkLookUp.EQ_FIELDS + "' or '" + RethinkLookUp.LT_FIELDS + "' or '" + RethinkLookUp.GT_FIELDS
		                + "' is mandatory.",
		        "", new String[] { "The fields from source component for equal to (=) comparision with lookup row." }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(RethinkLookUp.EQ_COLUMNS, General.NO, "",
		        new String[] { "The columns differ fields from lookup table which are to be compared with eq fields." },
		        "Number of " + RethinkLookUp.EQ_COLUMNS + " should be equal to number of " + RethinkLookUp.EQ_FIELDS + ".", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(RethinkLookUp.LT_FIELDS,
		        General.NO + ": Either '" + RethinkLookUp.EQ_FIELDS + "' or '" + RethinkLookUp.LT_FIELDS + "' or '" + RethinkLookUp.GT_FIELDS
		                + "' is mandatory.",
		        "", new String[] { "The fields from source component for less than (<) comparision with lookup row." }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(RethinkLookUp.LT_COLUMNS, General.NO, "",
		        new String[] { "The columns differ to fields from lookup table which are to be compared with lt fields." },
		        "Number of " + RethinkLookUp.LT_COLUMNS + " should be equal to number of " + RethinkLookUp.LT_FIELDS + ".", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(RethinkLookUp.GT_FIELDS,
		        General.NO + ": Either '" + RethinkLookUp.EQ_FIELDS + "' or '" + RethinkLookUp.LT_FIELDS + "' or '" + RethinkLookUp.GT_FIELDS
		                + "' is mandatory.",
		        "", new String[] { "The fields from source component for greater than (>) comparision with lookup row." }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(RethinkLookUp.GT_COLUMNS, General.NO, "",
		        new String[] { "The columns differ to fields from lookup table which are to be compared with gt fields." },
		        "Number of " + RethinkLookUp.GT_COLUMNS + " should be equal to number of " + RethinkLookUp.GT_FIELDS + ".", Type.LIST_OF_STRINGS);

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

		configDescriptionUtil.addConfigDescription(General.DECIMAL_PRECISIONS, General.NO, String.valueOf(General.DECIMAL_PRECISION),
		        new String[] { "Precisions for decimal type field." }, "", "Integer");
		configDescriptionUtil.addConfigDescription(General.DECIMAL_SCALES, General.NO, String.valueOf(General.DECIMAL_SCALE),
		        new String[] { "Scales for decimal type field." }, "", "Integer");

		configDescriptionUtil.addConfigDescription(Operation.General.ON_ZERO_FETCH, General.NO, "",
		        new String[] { "Below are the allowed on zero fetch options.", OnZeroFetchOperation.FAIL + ":It will fail the job on zero fetch.",
		                OnZeroFetchOperation.IGNORE + ":It will return the incoming data set where selected fields will be null.",
		                OnZeroFetchOperation.INSERT + ":It will insert the provided insert values." },
		        true, HTMLTextStyle.ORDERED_LIST);

		configDescriptionUtil.addConfigDescription(Operation.General.INSERT_VALUES,
		        General.YES + ": When " + Operation.General.ON_ZERO_FETCH + " is '" + OnZeroFetchOperation.INSERT + "'", "",
		        new String[] { "Insert values for the select fields" }, "Number of insert values should match number of select fields", Type.LIST_OF_STRINGS);

		addRethinkLRetrievalConfigDescription();
	}

	protected abstract void addRethinkLRetrievalConfigDescription() throws InvalidParameter;
}

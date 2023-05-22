package com.anvizent.elt.core.spark.operation.doc.helper;

import com.anvizent.elt.core.spark.constant.CacheMode;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.ArangoDBDefault;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.ArangoDBLookUp;
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
public abstract class ArangoDBRetrievalDocHelper extends DocHelper {

	public ArangoDBRetrievalDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(SQLNoSQL.HOST, General.YES, "", new String[] { "ArangoDB host for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.PORT_NUMBER, General.YES, "",
		        new String[] { "ArangoDB port for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.DB_NAME, General.NO, ArangoDBDefault.DB_NAME,
		        new String[] { "ArangoDB database name for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.USER_NAME, General.NO, ArangoDBDefault.USER,
		        new String[] { "ArangoDB username for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.PASSWORD, General.NO, "<EMPTY_STRING>",
		        new String[] { "ArangoDB password for connecting to the given database." });
		configDescriptionUtil.addConfigDescription(SQLNoSQL.TIMEOUT, General.NO, "" + ArangoDBDefault.TIMEOUT, new String[] { "ArangoDB connection timeout." });

		configDescriptionUtil.addConfigDescription(SQLNoSQL.TABLE, General.YES, "", new String[] { "The lookup table name." });

		configDescriptionUtil.addConfigDescription(Operation.General.SELECT_COLUMNS, General.YES, "",
		        new String[] { "The select fields to fetch from lookup table." }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(ArangoDBLookUp.SELECT_FIELD_TYPES, General.NO, "",
		        StringUtil.join(new String[] { "The select field's eqvivalent java data types." }, General.Type.ALLOWED_TYPES_AS_STRINGS),
		        "Number of " + ArangoDBLookUp.SELECT_FIELD_TYPES + " provided should be equals to number of " + Operation.General.SELECT_COLUMNS + ".",
		        Type.LIST_OF_TYPES, true, HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil.addConfigDescription(ArangoDBLookUp.SELECT_FIELD_DATE_FORMATS, General.NO, "",
		        new String[] { "To convert selected date fields in particular date format." }, "", Type.LIST_OF_STRINGS);
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

		configDescriptionUtil.addConfigDescription(Operation.ArangoDBLookUp.ORDER_BY_FIELDS, General.NO, "", new String[] { "Order By fields" }, "",
		        Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(ArangoDBLookUp.ORDER_BY_TYPE, General.NO, "ASC", new String[] { "Order By type (ASC/DESC)" }, "",
		        Type.LIST_OF_STRINGS);

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
		        General.YES + ": When " + Operation.General.ON_ZERO_FETCH + " is '" + OnZeroFetchOperation.INSERT + "'", "",
		        new String[] { "Insert values for the select fields" }, "Number of insert values should match number of select fields", Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(Operation.ArangoDBLookUp.WAIT_FOR_SYNC, General.NO, "true",
		        new String[] { "To make sure data is durable when an insert query returns." }, Type.BOOLEAN);

		addArangoDBRetrievalConfigDescription();
	}

	protected abstract void addArangoDBRetrievalConfigDescription() throws InvalidParameter;

}

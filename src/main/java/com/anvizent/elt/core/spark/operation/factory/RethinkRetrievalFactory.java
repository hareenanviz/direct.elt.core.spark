package com.anvizent.elt.core.spark.operation.factory;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.RethinkDBConnection;
import com.anvizent.elt.core.listener.common.connection.RethinkDBConnectionByTaskId;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.RethinkLookUp;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.listener.responder.ELTCoreJobResponder;
import com.anvizent.elt.core.spark.operation.config.bean.RethinkRetrievalConfigBean;
import com.anvizent.elt.core.spark.operation.service.RethinkRetrievalFunctionService;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.util.StructureUtil;
import com.anvizent.elt.core.spark.validator.Validator;
import com.rethinkdb.net.Connection;

/**
 * @author Hareen Bejjanki
 *
 */
@SuppressWarnings("unchecked")
public abstract class RethinkRetrievalFactory extends SimpleOperationFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected Component process(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		RethinkRetrievalConfigBean rethinkRetrievalConfigBean = (RethinkRetrievalConfigBean) configBean;

		try {
			if (rethinkRetrievalConfigBean.getCacheType().equals(CacheType.MEMCACHE)
			        || rethinkRetrievalConfigBean.getCacheType().equals(CacheType.ELASTIC_CACHE)) {
				throw new UnimplementedException("'" + rethinkRetrievalConfigBean.getCacheType() + "' is not implemented.");
			}

			if (rethinkRetrievalConfigBean.getSelectFieldTypes() == null || rethinkRetrievalConfigBean.getSelectFieldTypes().isEmpty()) {
				throw new UnimplementedException("Rethink LookUp/Fetcher is not implemented without '" + RethinkLookUp.SELECT_FIELD_TYPES + "'.");
			}

			return getComponent(rethinkRetrievalConfigBean, component, errorHandlerSink);
		} catch (Exception exception) {
			closeConnection(rethinkRetrievalConfigBean.getRethinkDBConnection());
			throw exception;
		}
	}

	private void closeConnection(RethinkDBConnection rethinkDBConnection)
	        throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		Connection connection = (Connection) ApplicationConnectionBean.getInstance()
		        .get(new RethinkDBConnectionByTaskId(rethinkDBConnection, null, TaskContext.getPartitionId()), false)[0];
		if (connection != null) {
			connection.close();
		}
	}

	private Component getComponent(RethinkRetrievalConfigBean rethinkRetrievalConfigBean, Component component, ErrorHandlerSink errorHandlerSink)
	        throws Exception {
		component.addELTCoreJobListenerIfNotExists(CacheType.EHCACHE.name(), new ELTCoreJobResponder());

		LinkedHashMap<String, AnvizentDataType> newStructure = getNewLookUpStructure(rethinkRetrievalConfigBean, component);

		setTimeZoneDetails(rethinkRetrievalConfigBean);
		setWhereFieldsAndColumns(rethinkRetrievalConfigBean);
		setInsertValues(rethinkRetrievalConfigBean);

		JavaRDD<HashMap<String, Object>> retrievalRDD = getRetrievalRDD(rethinkRetrievalConfigBean, component, newStructure, errorHandlerSink);

		return Component.createComponent(component.getSparkSession(), rethinkRetrievalConfigBean.getName(), retrievalRDD, newStructure);
	}

	private void setInsertValues(RethinkRetrievalConfigBean rethinkRetrievalConfigBean)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		if (rethinkRetrievalConfigBean.getInsertValues() == null || rethinkRetrievalConfigBean.getInsertValues().isEmpty()) {
			return;
		}

		ArrayList<Object> outputInsertValues = new ArrayList<>();
		ArrayList<Object> tableInsertValues = new ArrayList<>();

		for (int i = 0; i < rethinkRetrievalConfigBean.getInsertValues().size(); i++) {
			String selectDateFormat = RethinkRetrievalFunctionService.getSelectDateFormat(i, rethinkRetrievalConfigBean.getSelectFieldDateFormats());
			Object outputValue = RethinkRetrievalFunctionService.convertToSelectFieldType(rethinkRetrievalConfigBean.getInsertValues().get(i),
			        rethinkRetrievalConfigBean.getSelectFieldTypes().get(i).getJavaType(), selectDateFormat, rethinkRetrievalConfigBean.getTimeZoneOffset());

			if (rethinkRetrievalConfigBean.getSelectFieldTypes().get(i).getJavaType().equals(Date.class)) {
				tableInsertValues.add(TypeConversionUtil.dateToOffsetDateTypeConversion(outputValue, Date.class, OffsetDateTime.class,
				        rethinkRetrievalConfigBean.getTimeZoneOffset()));
			} else {
				tableInsertValues.add(outputValue);
			}

			outputInsertValues.add(outputValue);
		}

		rethinkRetrievalConfigBean.setOutputInsertValues(outputInsertValues);
		rethinkRetrievalConfigBean.setTableInsertValues(tableInsertValues);
	}

	private void setWhereFieldsAndColumns(RethinkRetrievalConfigBean rethinkRetrievalConfigBean) {
		rethinkRetrievalConfigBean.setWhereFields(
		        getWhereFields(rethinkRetrievalConfigBean.getEqFields(), rethinkRetrievalConfigBean.getLtFields(), rethinkRetrievalConfigBean.getGtFields()));
		rethinkRetrievalConfigBean.setWhereColumns(getWhereColumns(rethinkRetrievalConfigBean.getEqColumns(), rethinkRetrievalConfigBean.getLtColumns(),
		        rethinkRetrievalConfigBean.getGtColumns()));
	}

	private ArrayList<String> getWhereFields(ArrayList<String> eqFields, ArrayList<String> ltFields, ArrayList<String> gtFields) {
		ArrayList<String> whereFields = new ArrayList<>();
		putWhereFieldsAndColumns(whereFields, eqFields, ltFields, gtFields);
		return whereFields;
	}

	public static ArrayList<String> getWhereColumns(ArrayList<String> eqColumns, ArrayList<String> ltColumns, ArrayList<String> gtColumns) {
		ArrayList<String> whereColumns = new ArrayList<>();
		putWhereFieldsAndColumns(whereColumns, eqColumns, ltColumns, gtColumns);
		return whereColumns;
	}

	private static void putWhereFieldsAndColumns(ArrayList<String> object, ArrayList<String>... multiFields) {
		for (ArrayList<String> fields : multiFields) {
			if (fields != null) {
				object.addAll(fields);
			}
		}
	}

	private void setTimeZoneDetails(RethinkRetrievalConfigBean rethinkRetrievalConfigBean)
	        throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		ZoneOffset timeZoneOffset = OffsetDateTime.now().getOffset();
		rethinkRetrievalConfigBean.setTimeZoneOffset(timeZoneOffset);
	}

	private LinkedHashMap<String, AnvizentDataType> getNewLookUpStructure(RethinkRetrievalConfigBean rethinkRetrievalConfigBean, Component component)
	        throws InvalidConfigException {
		return StructureUtil.getNewStructure(rethinkRetrievalConfigBean, component.getStructure(),
		        rethinkRetrievalConfigBean.getSelectFieldAliases() == null || rethinkRetrievalConfigBean.getSelectFieldAliases().isEmpty()
		                ? rethinkRetrievalConfigBean.getSelectFields()
		                : rethinkRetrievalConfigBean.getSelectFieldAliases(),
		        rethinkRetrievalConfigBean.getSelectFieldTypes(), rethinkRetrievalConfigBean.getSelectFieldPositions(),
		        Operation.General.SELECT_COLUMNS_AS_FIELDS, Operation.General.SELECT_FIELD_POSITIONS);
	}

	@Override
	public Integer getMaxInputs() {
		return 1;
	}

	@Override
	public Integer getMinInputs() {
		return 1;
	}

	@Override
	public Validator getValidator() {
		return getRethinkRetrievalValidator();
	}

	@Override
	public SchemaValidator getSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return getRethinkRetrievalDocHelper();
	}

	@Override
	protected void createFactoryAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		AnvizentAccumulator inAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(), getName(),
		        StatsCategory.IN, StatsNames.IN, true, getStatsCalculator(StatsCategory.IN, StatsNames.IN, getName()));

		ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), inAnvizentAccumulator);

		if ((statsType.equals(StatsType.COMPONENT_LEVEL) && !componentLevel) || statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator outAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.OUT, !componentLevel, getStatsCalculator(StatsCategory.OUT, StatsNames.OUT, getName()));

			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), outAnvizentAccumulator);
		}
	}

	@Override
	protected void createSpecialAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		RethinkRetrievalConfigBean rethinkRetrievalConfigBean = (RethinkRetrievalConfigBean) configBean;

		AnvizentAccumulator lookupAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        getName(), StatsCategory.OTHER, StatsNames.LOOKUP_COUNT, true, getStatsCalculator(StatsCategory.OTHER, StatsNames.LOOKUP_COUNT, getName()));
		AnvizentAccumulator lookedupRowsAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        getName(), StatsCategory.OTHER, StatsNames.LOOKEDUP_ROWS, true, getStatsCalculator(StatsCategory.OTHER, StatsNames.LOOKEDUP_ROWS, getName()));

		ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), lookupAnvizentAccumulator, lookedupRowsAnvizentAccumulator);

		if (rethinkRetrievalConfigBean.getCacheType() != null && !rethinkRetrievalConfigBean.getCacheType().equals(CacheType.NONE)) {
			AnvizentAccumulator cachedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OTHER, StatsNames.CACHE, true, getStatsCalculator(StatsCategory.OTHER, StatsNames.CACHE, getName()));
			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), cachedAnvizentAccumulator);
		}

		if (rethinkRetrievalConfigBean.getOnZeroFetch().equals(OnZeroFetchOperation.INSERT)) {
			AnvizentAccumulator insertedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.INSERTED, true, getStatsCalculator(StatsCategory.OUT, StatsNames.INSERTED, getName()));
			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), insertedAnvizentAccumulator);
		}
	}

	protected abstract Validator getRethinkRetrievalValidator();

	protected abstract DocHelper getRethinkRetrievalDocHelper() throws InvalidParameter;

	protected abstract JavaRDD<HashMap<String, Object>> getRetrievalRDD(RethinkRetrievalConfigBean rethinkRetrievalConfigBean, Component component,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception;
}

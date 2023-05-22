package com.anvizent.elt.core.spark.operation.factory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.SQLUtil;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.factory.RetryMandatoryFactory;
import com.anvizent.elt.core.spark.listener.responder.ELTCoreJobResponder;
import com.anvizent.elt.core.spark.operation.config.bean.SQLRetrievalConfigBean;
import com.anvizent.elt.core.spark.operation.schema.validator.SQLRetrievalSchemaValidator;
import com.anvizent.elt.core.spark.operation.service.SQLRetrievalFactoryService;
import com.anvizent.elt.core.spark.operation.validator.SQLRetrievalValidator;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.util.StructureUtil;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class SQLRetrievalFactory extends SimpleOperationFactory implements RetryMandatoryFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected Component process(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		SQLRetrievalConfigBean sqlRetrievalConfigBean = (SQLRetrievalConfigBean) configBean;

		if (sqlRetrievalConfigBean.getCacheType().equals(CacheType.MEMCACHE) || sqlRetrievalConfigBean.getCacheType().equals(CacheType.ELASTIC_CACHE)) {
			throw new UnimplementedException("'" + sqlRetrievalConfigBean.getCacheType() + "' is not implemented.");
		}

		return getComponent(sqlRetrievalConfigBean, component, errorHandlerSink);
	}

	private Component getComponent(SQLRetrievalConfigBean sqlRetrievalConfigBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		component.addELTCoreJobListenerIfNotExists(CacheType.EHCACHE.name(), new ELTCoreJobResponder());

		LinkedHashMap<String, AnvizentDataType> newStructure = getNewLookUpStructure(sqlRetrievalConfigBean, component);

		buildQueries(sqlRetrievalConfigBean);

		JavaRDD<HashMap<String, Object>> retrievalRDD = getRetrievalRDD(sqlRetrievalConfigBean, component, newStructure, errorHandlerSink);

		return Component.createComponent(component.getSparkSession(), sqlRetrievalConfigBean.getName(), retrievalRDD, newStructure);
	}

	private void buildQueries(SQLRetrievalConfigBean sqlRetrievalConfigBean) throws UnimplementedException, InvalidInputForConfigException {
		ArrayList<String> selectColumns;
		ArrayList<String> selectColumnsAliases;

		if (StringUtils.isBlank(sqlRetrievalConfigBean.getCustomWhere())) {
			selectColumns = getSelectAndWhereFields(sqlRetrievalConfigBean.getSelectColumns(),
			        sqlRetrievalConfigBean.getWhereColumns() == null || sqlRetrievalConfigBean.getWhereColumns().isEmpty()
			                ? sqlRetrievalConfigBean.getWhereFields()
			                : sqlRetrievalConfigBean.getWhereColumns());
			selectColumnsAliases = getSelectAndWhereFields(sqlRetrievalConfigBean.getSelectFieldAliases(), sqlRetrievalConfigBean.getWhereFields());
		} else {
			selectColumns = sqlRetrievalConfigBean.getSelectColumns();
			selectColumnsAliases = sqlRetrievalConfigBean.getSelectFieldAliases();
		}

		buildSelectQuery(sqlRetrievalConfigBean, selectColumns, selectColumnsAliases);
		SQLRetrievalFactoryService.buildInsertQuery(sqlRetrievalConfigBean);

		if (sqlRetrievalConfigBean.getCacheType().equals(CacheType.EHCACHE)) {
			buildBulkSelectQuery(sqlRetrievalConfigBean, selectColumns, selectColumnsAliases);
		}

		sqlRetrievalConfigBean.setSelectAndWhereFields(selectColumnsAliases == null || selectColumnsAliases.isEmpty() ? selectColumns : selectColumnsAliases);
	}

	private void buildSelectQuery(SQLRetrievalConfigBean sqlRetrievalConfigBean, ArrayList<String> selectColumns, ArrayList<String> selectColumnAliases)
	        throws UnimplementedException, InvalidInputForConfigException {
		if (sqlRetrievalConfigBean.getCustomWhere() == null || sqlRetrievalConfigBean.getCustomWhere().isEmpty()) {
			sqlRetrievalConfigBean.setSelectQuery(SQLUtil.buildSelectQueryWithWhereColumns(sqlRetrievalConfigBean.getRdbmsConnection().getDriver(),
			        sqlRetrievalConfigBean.getTableName(), selectColumns, selectColumnAliases,
			        (sqlRetrievalConfigBean.getWhereColumns() == null || sqlRetrievalConfigBean.getWhereColumns().isEmpty())
			                ? sqlRetrievalConfigBean.getWhereFields()
			                : sqlRetrievalConfigBean.getWhereColumns(),
			        sqlRetrievalConfigBean.getOrderBy(), sqlRetrievalConfigBean.getOrderByTypes(), sqlRetrievalConfigBean.isKeyFieldsCaseSensitive()));

		} else {
			sqlRetrievalConfigBean.setSelectQuery(SQLUtil.buildSelectQueryWithCustomWhere(sqlRetrievalConfigBean.getRdbmsConnection().getDriver(),
			        sqlRetrievalConfigBean.getTableName(), selectColumns, selectColumnAliases, sqlRetrievalConfigBean.getCustomQueryAfterProcess(),
			        sqlRetrievalConfigBean.getOrderBy(), sqlRetrievalConfigBean.getOrderByTypes(), sqlRetrievalConfigBean.isKeyFieldsCaseSensitive()));
		}
	}

	private void buildBulkSelectQuery(SQLRetrievalConfigBean sqlRetrievalConfigBean, ArrayList<String> selectFields, ArrayList<String> selectFieldsAliases)
	        throws UnimplementedException, InvalidInputForConfigException {
		if (sqlRetrievalConfigBean.getCustomWhere() == null || sqlRetrievalConfigBean.getCustomWhere().isEmpty()) {
			sqlRetrievalConfigBean.setBulkSelectQuery(SQLUtil.buildBulkSelectQueryWithWhereColumns(sqlRetrievalConfigBean.getRdbmsConnection().getDriver(),
			        sqlRetrievalConfigBean.getTableName(), selectFields, selectFieldsAliases,
			        (sqlRetrievalConfigBean.getWhereColumns() == null || sqlRetrievalConfigBean.getWhereColumns().isEmpty())
			                ? sqlRetrievalConfigBean.getWhereFields()
			                : sqlRetrievalConfigBean.getWhereColumns(),
			        sqlRetrievalConfigBean.getOrderBy(), sqlRetrievalConfigBean.getOrderByTypes(), sqlRetrievalConfigBean.isKeyFieldsCaseSensitive(),
			        sqlRetrievalConfigBean.getMaxElementsInMemory()));
		}
	}

	private ArrayList<String> getSelectAndWhereFields(ArrayList<String> selectFields, ArrayList<String> whereColumns) {
		ArrayList<String> fields = new ArrayList<>();
		if (selectFields != null && !selectFields.isEmpty()) {
			fields.addAll(selectFields);
			if (whereColumns != null && !whereColumns.isEmpty()) {
				fields.addAll(whereColumns);
			}
		}

		return fields;
	}

	private LinkedHashMap<String, AnvizentDataType> getNewLookUpStructure(SQLRetrievalConfigBean sqlRetrievalConfigBean, Component component)
	        throws ImproperValidationException, ClassNotFoundException, InvalidConfigException, SQLException, UnsupportedException, UnimplementedException,
	        TimeoutException, InvalidInputForConfigException {
		ArrayList<AnvizentDataType> selectFieldDataTypes = SQLRetrievalFactoryService.getSelectFieldDataTypes(sqlRetrievalConfigBean);

		return StructureUtil.getNewStructure(sqlRetrievalConfigBean, component.getStructure(),
		        sqlRetrievalConfigBean.getSelectFieldAliases() == null || sqlRetrievalConfigBean.getSelectFieldAliases().isEmpty()
		                ? sqlRetrievalConfigBean.getSelectColumns()
		                : sqlRetrievalConfigBean.getSelectFieldAliases(),
		        selectFieldDataTypes, sqlRetrievalConfigBean.getSelectFieldPositions(), Operation.General.SELECT_COLUMNS_AS_FIELDS,
		        Operation.General.SELECT_FIELD_POSITIONS);
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
	public SQLRetrievalValidator getValidator() {
		return getSQLRetrievalValidator();
	}

	@Override
	public SchemaValidator getSchemaValidator() {
		return new SQLRetrievalSchemaValidator();
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
		SQLRetrievalConfigBean sqlRetrievalConfigBean = (SQLRetrievalConfigBean) configBean;

		AnvizentAccumulator lookupAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        getName(), StatsCategory.OTHER, StatsNames.LOOKUP_COUNT, true, getStatsCalculator(StatsCategory.OTHER, StatsNames.LOOKUP_COUNT, getName()));
		AnvizentAccumulator lookedupRowsAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        getName(), StatsCategory.OTHER, StatsNames.LOOKEDUP_ROWS, true, getStatsCalculator(StatsCategory.OTHER, StatsNames.LOOKEDUP_ROWS, getName()));

		ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), lookupAnvizentAccumulator, lookedupRowsAnvizentAccumulator);

		if (sqlRetrievalConfigBean.getCacheType() != null && !sqlRetrievalConfigBean.getCacheType().equals(CacheType.NONE)) {
			AnvizentAccumulator cachedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OTHER, StatsNames.CACHE, true, getStatsCalculator(StatsCategory.OTHER, StatsNames.CACHE, getName()));
			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), cachedAnvizentAccumulator);
		}

		if (sqlRetrievalConfigBean.getOnZeroFetch().equals(OnZeroFetchOperation.INSERT)) {
			AnvizentAccumulator insertedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.INSERTED, true, getStatsCalculator(StatsCategory.OUT, StatsNames.INSERTED, getName()));
			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), insertedAnvizentAccumulator);
		}
	}

	protected abstract JavaRDD<HashMap<String, Object>> getRetrievalRDD(SQLRetrievalConfigBean sqlRetrievalConfigBean, Component component,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ErrorHandlerSink errorHandlerSink) throws Exception;

	protected abstract SQLRetrievalValidator getSQLRetrievalValidator();
}

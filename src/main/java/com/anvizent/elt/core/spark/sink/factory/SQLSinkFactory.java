package com.anvizent.elt.core.spark.sink.factory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.spark.TaskContext;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.BaseAnvizentBatchFunction;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentSQLBatchStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentVoidStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.SQLBatchSinkStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.SQLSinkStatsCalculator;
import com.anvizent.elt.core.listener.ApplicationListener;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnectionByTaskId;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.common.util.QueryInfo;
import com.anvizent.elt.core.spark.common.util.SQLUtil;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.constant.BatchType;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink.Components;
import com.anvizent.elt.core.spark.constant.DBCheckMode;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.constant.DBWriteMode;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.exception.TableAlreadyExistsException;
import com.anvizent.elt.core.spark.factory.RetryMandatoryFactory;
import com.anvizent.elt.core.spark.listener.responder.ELTCoreJobResponder;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;
import com.anvizent.elt.core.spark.sink.doc.helper.SQLSinkDocHelper;
import com.anvizent.elt.core.spark.sink.factory.util.SQLSinkFactoryDBQueryUtil;
import com.anvizent.elt.core.spark.sink.factory.util.SQLSinkFactoryPrefetchUtil;
import com.anvizent.elt.core.spark.sink.function.SQLSinkFunction;
import com.anvizent.elt.core.spark.sink.function.SQLSinkInserIfNotExistsFunction;
import com.anvizent.elt.core.spark.sink.function.SQLSinkInsertBatchFunction;
import com.anvizent.elt.core.spark.sink.function.SQLSinkInsertFunction;
import com.anvizent.elt.core.spark.sink.function.SQLSinkInsertIfNotExistsBatchFunction;
import com.anvizent.elt.core.spark.sink.function.SQLSinkNoKeysInsertBatchFunction;
import com.anvizent.elt.core.spark.sink.function.SQLSinkUpdateBatchFunction;
import com.anvizent.elt.core.spark.sink.function.SQLSinkUpdateFunction;
import com.anvizent.elt.core.spark.sink.function.SQLSinkUpsertBatchFunction;
import com.anvizent.elt.core.spark.sink.function.SQLSinkUpsertFunction;
import com.anvizent.elt.core.spark.sink.schema.validator.SQLSinkSchemaValidator;
import com.anvizent.elt.core.spark.sink.service.SQLSinkService;
import com.anvizent.elt.core.spark.sink.validator.SQLSinkValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;
import com.anvizent.query.builder.exception.UnderConstructionException;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkFactory extends SinkFactory implements RetryMandatoryFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected void write(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		SQLSinkConfigBean sqlSinkConfigBean = (SQLSinkConfigBean) configBean;

		beforeComponentQuery(sqlSinkConfigBean);

		SQLSinkService.setFields(sqlSinkConfigBean, component.getStructure());

		if (!checkAndCreateTable(sqlSinkConfigBean, component.getStructure())) {
			return;
		}

		ApplicationListener.getInstance().getComponents().get(component.getName()).addELTCoreJobListenerIfNotExists(CacheType.EHCACHE.name(),
		        new ELTCoreJobResponder());

		AnvizentVoidFunction errorHandlerSinkFunction = ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink,
		        getName());
		JobDetails jobDetails = ErrorHandlerUtil.getJobDetails(configBean, getName());
		ArrayList<AnvizentAccumulator> anvizentAccumulators = ApplicationBean.getInstance().getAccumulators(configBean.getName(), getName());

		if (sqlSinkConfigBean.getDBCheckMode().equals(DBCheckMode.DB_QUERY)) {
			new SQLSinkFactoryDBQueryUtil(sqlSinkConfigBean, component.getStructure(), component.getRDD(sqlSinkConfigBean.getSourceStream()))
			        .write(anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
		} else if (sqlSinkConfigBean.getDBCheckMode().equals(DBCheckMode.LOAD_ALL)) {
			// TODO
		} else {
			if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT)
			        || sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT_IF_NOT_EXISTS)) {
				SQLSinkService.buildInsertQuery(sqlSinkConfigBean);
			} else if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)) {
				SQLSinkService.buildUpdateQuery(sqlSinkConfigBean);
			} else {
				SQLSinkService.buildUpsertQueries(sqlSinkConfigBean);
			}

			if (sqlSinkConfigBean.getDBCheckMode().equals(DBCheckMode.PREFECTH)) {
				new SQLSinkFactoryPrefetchUtil(sqlSinkConfigBean, component.getStructure(), component.getRDD(sqlSinkConfigBean.getSourceStream()))
				        .write(anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
			} else {
				SQLSinkService.buildSelectQuery(sqlSinkConfigBean, new ArrayList<>(component.getStructure().keySet()));

				write(sqlSinkConfigBean, anvizentAccumulators, component, errorHandlerSinkFunction, jobDetails);
			}
		}

		afterComponentQuery(sqlSinkConfigBean);
	}

	private void beforeComponentQuery(SQLSinkConfigBean sqlSinkConfigBean)
	        throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		Connection connection = (Connection) ApplicationConnectionBean.getInstance()
		        .get(new RDBMSConnectionByTaskId(sqlSinkConfigBean.getRdbmsConnection(), null, TaskContext.getPartitionId()), true)[0];
		SQLUtil.executeQuey(sqlSinkConfigBean.getBeforeComponentRunQuery(), connection);
	}

	private void afterComponentQuery(SQLSinkConfigBean sqlSinkConfigBean)
	        throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		Connection connection = (Connection) ApplicationConnectionBean.getInstance()
		        .get(new RDBMSConnectionByTaskId(sqlSinkConfigBean.getRdbmsConnection(), null, TaskContext.getPartitionId()), true)[0];
		SQLUtil.executeQuey(sqlSinkConfigBean.getAfterComponentSuccessRunQuery(), connection);
	}

	private void write(SQLSinkConfigBean sqlSinkConfigBean, ArrayList<AnvizentAccumulator> anvizentAccumulators, Component component,
	        AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws InvalidRelationException, InvalidArgumentsException, UnimplementedException, InvalidInputForConfigException, UnderConstructionException {
		if (sqlSinkConfigBean.getBatchType().equals(BatchType.NONE)) {
			recordProcessing(sqlSinkConfigBean, anvizentAccumulators, component, errorHandlerSinkFunction, jobDetails);
		} else {
			batchProcessing(sqlSinkConfigBean, anvizentAccumulators, component, errorHandlerSinkFunction, jobDetails);
		}
	}

	private void batchProcessing(SQLSinkConfigBean sqlSinkConfigBean, ArrayList<AnvizentAccumulator> anvizentAccumulators, Component component,
	        AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws InvalidRelationException, InvalidArgumentsException, UnimplementedException, InvalidInputForConfigException, UnderConstructionException {
		if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT) || sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT_IF_NOT_EXISTS)) {
			SQLSinkService.buildInsertQuery(sqlSinkConfigBean);
		} else if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)) {
			SQLSinkService.buildUpdateQuery(sqlSinkConfigBean);
		} else {
			SQLSinkService.buildUpsertQueries(sqlSinkConfigBean);
		}

		QueryInfo deleteQueryInfo = SQLSinkService.buildDeleteQueries(component.getStructure().keySet(), sqlSinkConfigBean);

		if (deleteQueryInfo != null) {
			sqlSinkConfigBean.setDeleteQuery(deleteQueryInfo.getQuery());
		}

		BaseAnvizentBatchFunction<HashMap<String, Object>, HashMap<String, Object>> sqlSinkFunction = null;

		if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT)
		        && (sqlSinkConfigBean.getKeyFields() == null || sqlSinkConfigBean.getKeyFields().isEmpty())) {
			sqlSinkFunction = new SQLSinkNoKeysInsertBatchFunction(sqlSinkConfigBean, component.getStructure(), anvizentAccumulators, errorHandlerSinkFunction,
			        jobDetails);
		} else if (sqlSinkConfigBean.getDBCheckMode().equals(DBCheckMode.REGULAR)) {
			if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT)) {
				sqlSinkFunction = new SQLSinkInsertBatchFunction(sqlSinkConfigBean, component.getStructure(), anvizentAccumulators, errorHandlerSinkFunction,
				        jobDetails);
			} else if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)) {
				sqlSinkFunction = new SQLSinkUpdateBatchFunction(sqlSinkConfigBean, component.getStructure(), anvizentAccumulators, errorHandlerSinkFunction,
				        jobDetails);
			} else if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
				sqlSinkFunction = new SQLSinkUpsertBatchFunction(sqlSinkConfigBean, component.getStructure(), anvizentAccumulators, errorHandlerSinkFunction,
				        jobDetails);
			} else {
				sqlSinkFunction = new SQLSinkInsertIfNotExistsBatchFunction(sqlSinkConfigBean, component.getStructure(), anvizentAccumulators,
				        errorHandlerSinkFunction, jobDetails);
			}
		}

		component.getRDD(sqlSinkConfigBean.getSourceStream()).foreachPartition(sqlSinkFunction);
	}

	private void recordProcessing(SQLSinkConfigBean sqlSinkConfigBean, ArrayList<AnvizentAccumulator> anvizentAccumulators, Component component,
	        AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws InvalidRelationException, UnimplementedException, InvalidInputForConfigException {
		SQLSinkFunction sqlSinkFunction;

		if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT)) {
			SQLSinkService.buildInsertQuery(sqlSinkConfigBean);
			sqlSinkFunction = new SQLSinkInsertFunction(sqlSinkConfigBean, component.getStructure(), anvizentAccumulators, errorHandlerSinkFunction,
			        jobDetails);
		} else if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)) {
			SQLSinkService.buildUpdateQuery(sqlSinkConfigBean);
			sqlSinkFunction = new SQLSinkUpdateFunction(sqlSinkConfigBean, component.getStructure(), anvizentAccumulators, errorHandlerSinkFunction,
			        jobDetails);
		} else if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			SQLSinkService.buildUpsertQueries(sqlSinkConfigBean);
			sqlSinkFunction = new SQLSinkUpsertFunction(sqlSinkConfigBean, component.getStructure(), anvizentAccumulators, errorHandlerSinkFunction,
			        jobDetails);
		} else {
			SQLSinkService.buildInsertQuery(sqlSinkConfigBean, sqlSinkConfigBean.getConstantsConfigBean().getColumns(),
			        sqlSinkConfigBean.getConstantsConfigBean().getValues());
			sqlSinkFunction = new SQLSinkInserIfNotExistsFunction(sqlSinkConfigBean, component.getStructure(), anvizentAccumulators, errorHandlerSinkFunction,
			        jobDetails);
		}

		component.getRDD(sqlSinkConfigBean.getSourceStream()).foreach(sqlSinkFunction);
	}

	private boolean checkAndCreateTable(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure)
	        throws ImproperValidationException, SQLException, UnsupportedException, TableAlreadyExistsException, InvalidConfigException, UnimplementedException,
	        TimeoutException, InvalidInputForConfigException {

		boolean tableExits = SQLSinkService.checkIfTableExists(sqlSinkConfigBean, structure);

		if (sqlSinkConfigBean.getDBWriteMode().equals(DBWriteMode.APPEND)) {
			append(sqlSinkConfigBean, structure, tableExits);
		} else if (sqlSinkConfigBean.getDBWriteMode().equals(DBWriteMode.IGNORE)) {
			return ignore(sqlSinkConfigBean, structure, tableExits);
		} else if (sqlSinkConfigBean.getDBWriteMode().equals(DBWriteMode.OVERWRITE)) {
			overwrite(sqlSinkConfigBean, structure, tableExits);
		} else if (sqlSinkConfigBean.getDBWriteMode().equals(DBWriteMode.TRUNCATE)) {
			truncate(sqlSinkConfigBean);
		} else {
			fail(sqlSinkConfigBean, structure, tableExits);
		}

		return true;
	}

	private void truncate(SQLSinkConfigBean sqlSinkConfigBean)
	        throws ImproperValidationException, UnimplementedException, SQLException, UnsupportedException, TimeoutException, InvalidInputForConfigException {
		SQLSinkService.truncate(sqlSinkConfigBean);
	}

	private void append(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure, boolean isTableExits)
	        throws ImproperValidationException, SQLException, UnsupportedException, InvalidConfigException, UnimplementedException, TimeoutException,
	        InvalidInputForConfigException {
		if (!isTableExits) {
			SQLSinkService.createTable(sqlSinkConfigBean, structure, getName());
		}
	}

	private boolean ignore(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure, boolean isTableExits)
	        throws ImproperValidationException, SQLException, UnsupportedException, InvalidConfigException, UnimplementedException, TimeoutException,
	        InvalidInputForConfigException {
		if (isTableExits) {
			return false;
		} else {
			SQLSinkService.createTable(sqlSinkConfigBean, structure, getName());
			return true;
		}
	}

	private void overwrite(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure, boolean tableExits)
	        throws ImproperValidationException, SQLException, UnsupportedException, InvalidConfigException, UnimplementedException, TimeoutException,
	        InvalidInputForConfigException {
		if (tableExits) {
			SQLSinkService.dropTable(sqlSinkConfigBean);
		}

		SQLSinkService.createTable(sqlSinkConfigBean, structure, getName());
	}

	private void fail(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure, boolean isTableExits)
	        throws SQLException, ImproperValidationException, UnsupportedException, TableAlreadyExistsException, InvalidConfigException, UnimplementedException,
	        TimeoutException, InvalidInputForConfigException {
		if (isTableExits) {
			throw new TableAlreadyExistsException("Table '" + sqlSinkConfigBean.getTableName() + "' already exists");
		} else {
			SQLSinkService.createTable(sqlSinkConfigBean, structure, getName());
		}
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new SQLSinkDocHelper(this);
	}

	@Override
	public String getName() {
		return Components.SQL_SINK.get(General.NAME);
	}

	@Override
	public Validator getValidator() {
		return new SQLSinkValidator(this);
	}

	@Override
	public SchemaValidator getSchemaValidator() {
		return new SQLSinkSchemaValidator();
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
	protected IStatsCalculator getStatsCalculator(StatsCategory statsCategory, String statsName, String internalRDDName) {
		return new AnvizentVoidStatsCalculator(statsCategory, statsName);
	}

	@Override
	public void createAccumulators(ConfigAndMappingConfigBeans configAndMappingConfigBeans, StatsType globalStatsType) {
		SQLSinkConfigBean sqlSinkConfigBean = (SQLSinkConfigBean) configAndMappingConfigBeans.getConfigBean();

		if (sqlSinkConfigBean.getBatchType().equals(BatchType.NONE)) {
			super.createAccumulators(configAndMappingConfigBeans, globalStatsType);
		} else {
			createBatchAccumulators(configAndMappingConfigBeans, globalStatsType);
		}
	}

	@Override
	protected void createSpecialAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		if (((SQLSinkConfigBean) configBean).getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			AnvizentAccumulator insertedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.INSERTED, true, new SQLSinkStatsCalculator(StatsCategory.OUT, StatsNames.INSERTED));

			AnvizentAccumulator updatedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.UPDATED, true, new SQLSinkStatsCalculator(StatsCategory.OUT, StatsNames.UPDATED));

			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), insertedAnvizentAccumulator, updatedAnvizentAccumulator);
		}
	}

	private void createBatchAccumulators(ConfigAndMappingConfigBeans configAndMappingConfigBeans, StatsType globalStatsType) {
		StatsType statsType = getStatsType(configAndMappingConfigBeans.getStatsStore(), globalStatsType);
		boolean componentLevel = isComponentLevel(configAndMappingConfigBeans.getMappingConfigBeans());
		ConfigBean configBean = configAndMappingConfigBeans.getConfigBean();

		if (statsType.equals(StatsType.NONE)) {
			return;
		} else if ((statsType.equals(StatsType.COMPONENT_LEVEL) && !componentLevel) || statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator inAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.IN, StatsNames.IN, !componentLevel, new AnvizentSQLBatchStatsCalculator(StatsCategory.IN, StatsNames.IN));
			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), inAnvizentAccumulator);
		}

		AnvizentAccumulator writtenAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        getName(), StatsCategory.OUT, StatsNames.WRITTEN, true, new AnvizentSQLBatchStatsCalculator(StatsCategory.OUT, StatsNames.WRITTEN));
		AnvizentAccumulator errorAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(), getName(),
		        StatsCategory.ERROR, StatsNames.ERROR, true, new AnvizentSQLBatchStatsCalculator(StatsCategory.ERROR, StatsNames.ERROR));

		ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), writtenAnvizentAccumulator, errorAnvizentAccumulator);

		if (((SQLSinkConfigBean) configBean).getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			AnvizentAccumulator insertedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.INSERTED, true, new SQLBatchSinkStatsCalculator(StatsCategory.OUT, StatsNames.INSERTED));

			AnvizentAccumulator updatedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.UPDATED, true, new SQLBatchSinkStatsCalculator(StatsCategory.OUT, StatsNames.UPDATED));

			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), insertedAnvizentAccumulator, updatedAnvizentAccumulator);
		}
	}

	@Override
	public ResourceValidator getResourceConfigValidator() {
		// TODO Auto-generated method stub
		return null;
	}
}

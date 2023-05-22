package com.anvizent.elt.core.spark.sink.factory;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentSQLBatchStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentVoidStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.SQLBatchSinkStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.SQLSinkStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.constant.BatchType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink.Components;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.constant.DBWriteMode;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.exception.TableAlreadyExistsException;
import com.anvizent.elt.core.spark.factory.RetryMandatoryFactory;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.sink.config.bean.RethinkDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.doc.helper.RethinkDBSinkDocHelper;
import com.anvizent.elt.core.spark.sink.function.RethinkDBSinkBatchFunction;
import com.anvizent.elt.core.spark.sink.function.RethinkDBSinkFunction;
import com.anvizent.elt.core.spark.sink.function.RethinkDBSinkInsertFunction;
import com.anvizent.elt.core.spark.sink.function.RethinkDBSinkInsertIfNotExistsFunction;
import com.anvizent.elt.core.spark.sink.function.RethinkDBSinkUpdateFunction;
import com.anvizent.elt.core.spark.sink.function.RethinkDBSinkUpsertFunction;
import com.anvizent.elt.core.spark.sink.service.RethinkDBSinkService;
import com.anvizent.elt.core.spark.sink.validator.RethinkDBSinkValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class RethinkDBSinkFactory extends SinkFactory implements RetryMandatoryFactory {
	private static final long serialVersionUID = 1L;

	@Override
	protected void write(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		RethinkDBSinkConfigBean rethinkDBSinkConfigBean = (RethinkDBSinkConfigBean) configBean;

		try {
			LinkedHashMap<String, AnvizentDataType> newStructure = RethinkDBSinkService.getNewStructure(rethinkDBSinkConfigBean, component);

			if (!checkAndCreateTable(rethinkDBSinkConfigBean)) {
				return;
			}

			RethinkDBSinkService.setSelectFields(rethinkDBSinkConfigBean, component);
			RethinkDBSinkService.setTimeZoneDetails(rethinkDBSinkConfigBean);
			AnvizentVoidFunction errorHandlerSinkFunction = ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink,
			        getName());
			JobDetails jobDetails = ErrorHandlerUtil.getJobDetails(configBean, getName());

			write(rethinkDBSinkConfigBean, ApplicationBean.getInstance().getAccumulators(configBean.getName(), getName()), component, newStructure,
			        errorHandlerSinkFunction, jobDetails);
		} finally {
			RethinkDBSinkService.closeConnection(rethinkDBSinkConfigBean.getConnection());
		}
	}

	private void write(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, ArrayList<AnvizentAccumulator> anvizentAccumulators, Component component,
	        LinkedHashMap<String, AnvizentDataType> newStructure, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws UnimplementedException, InvalidRelationException, InvalidArgumentsException {
		if (rethinkDBSinkConfigBean.getBatchType().equals(BatchType.NONE)) {
			recordProcessing(rethinkDBSinkConfigBean, anvizentAccumulators, component, newStructure, errorHandlerSinkFunction, jobDetails);
		} else {
			batchProcessing(rethinkDBSinkConfigBean, anvizentAccumulators, component, newStructure, errorHandlerSinkFunction, jobDetails);
		}
	}

	private void batchProcessing(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, ArrayList<AnvizentAccumulator> anvizentAccumulators, Component component,
	        LinkedHashMap<String, AnvizentDataType> newStructure, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws InvalidRelationException, InvalidArgumentsException {
		component.getRDD(rethinkDBSinkConfigBean.getSourceStream()).foreachPartition(new RethinkDBSinkBatchFunction(rethinkDBSinkConfigBean,
		        component.getStructure(), newStructure, anvizentAccumulators, errorHandlerSinkFunction, jobDetails));
	}

	private void recordProcessing(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, ArrayList<AnvizentAccumulator> anvizentAccumulators, Component component,
	        LinkedHashMap<String, AnvizentDataType> newStructure, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws UnimplementedException, InvalidRelationException {
		RethinkDBSinkFunction rethinkDBSinkFunction;

		if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT)) {
			rethinkDBSinkFunction = new RethinkDBSinkInsertFunction(rethinkDBSinkConfigBean, component.getStructure(), newStructure, anvizentAccumulators,
			        errorHandlerSinkFunction, jobDetails);
		} else if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)) {
			rethinkDBSinkFunction = new RethinkDBSinkUpdateFunction(rethinkDBSinkConfigBean, component.getStructure(), newStructure, anvizentAccumulators,
			        errorHandlerSinkFunction, jobDetails);
		} else if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			rethinkDBSinkFunction = new RethinkDBSinkUpsertFunction(rethinkDBSinkConfigBean, component.getStructure(), newStructure, anvizentAccumulators,
			        errorHandlerSinkFunction, jobDetails);
		} else if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT_IF_NOT_EXISTS)) {
			rethinkDBSinkFunction = new RethinkDBSinkInsertIfNotExistsFunction(rethinkDBSinkConfigBean, component.getStructure(), newStructure,
			        anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
		} else {
			throw new UnimplementedException();
		}

		component.getRDD(rethinkDBSinkConfigBean.getSourceStream()).foreach(rethinkDBSinkFunction);
	}

	public boolean checkAndCreateTable(RethinkDBSinkConfigBean rethinkDBSinkConfigBean) throws Exception {

		boolean tableExists = RethinkDBSinkService.checkIfTableExists(rethinkDBSinkConfigBean);

		if (rethinkDBSinkConfigBean.getDBWriteMode().equals(DBWriteMode.APPEND)) {
			append(rethinkDBSinkConfigBean, tableExists);
		} else if (rethinkDBSinkConfigBean.getDBWriteMode().equals(DBWriteMode.IGNORE)) {
			return ignore(rethinkDBSinkConfigBean, tableExists);
		} else if (rethinkDBSinkConfigBean.getDBWriteMode().equals(DBWriteMode.OVERWRITE)) {
			overwrite(rethinkDBSinkConfigBean, tableExists);
		} else if (rethinkDBSinkConfigBean.getDBWriteMode().equals(DBWriteMode.TRUNCATE)) {
			truncate(rethinkDBSinkConfigBean);
		} else {
			fail(rethinkDBSinkConfigBean, tableExists);
		}

		return true;
	}

	private void append(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, boolean isTableExits) throws Exception {
		if (!isTableExits) {
			RethinkDBSinkService.createTable(rethinkDBSinkConfigBean);
		}
	}

	private boolean ignore(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, boolean isTableExits) throws Exception {
		if (isTableExits) {
			return false;
		} else {
			RethinkDBSinkService.createTable(rethinkDBSinkConfigBean);
			return true;
		}
	}

	private void overwrite(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, boolean tableExits) throws Exception {
		if (tableExits) {
			RethinkDBSinkService.dropTable(rethinkDBSinkConfigBean);
		}

		RethinkDBSinkService.createTable(rethinkDBSinkConfigBean);
	}

	private void truncate(RethinkDBSinkConfigBean rethinkDBSinkConfigBean) throws Exception {
		RethinkDBSinkService.truncate(rethinkDBSinkConfigBean);
	}

	private void fail(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, boolean isTableExits) throws Exception {
		if (isTableExits) {
			throw new TableAlreadyExistsException("Table '" + rethinkDBSinkConfigBean.getTableName() + "' already exists");
		} else {
			RethinkDBSinkService.createTable(rethinkDBSinkConfigBean);
		}
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new RethinkDBSinkDocHelper(this);
	}

	@Override
	public String getName() {
		return Components.RETHINK_DB_SINK.get(General.NAME);
	}

	@Override
	public Validator getValidator() {
		return new RethinkDBSinkValidator(this);
	}

	@Override
	public SchemaValidator getSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
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
		RethinkDBSinkConfigBean rethinkSinkConfigBean = (RethinkDBSinkConfigBean) configAndMappingConfigBeans.getConfigBean();

		if (rethinkSinkConfigBean.getBatchType().equals(BatchType.NONE)) {
			super.createAccumulators(configAndMappingConfigBeans, globalStatsType);
		} else {
			createBatchAccumulators(configAndMappingConfigBeans, globalStatsType);
		}
	}

	@Override
	protected void createSpecialAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		if (((RethinkDBSinkConfigBean) configBean).getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			AnvizentAccumulator insertedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.INSERTED, true, new SQLSinkStatsCalculator(StatsCategory.OUT, StatsNames.INSERTED));

			AnvizentAccumulator updatedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.UPDATED, true, new SQLSinkStatsCalculator(StatsCategory.OUT, StatsNames.UPDATED));

			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), insertedAnvizentAccumulator, updatedAnvizentAccumulator);
		} else if (((RethinkDBSinkConfigBean) configBean).getDBInsertMode().equals(DBInsertMode.UPDATE)) {
			AnvizentAccumulator updatedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.UPDATED, true, new SQLSinkStatsCalculator(StatsCategory.OUT, StatsNames.UPDATED));

			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), updatedAnvizentAccumulator);
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

		if (((RethinkDBSinkConfigBean) configBean).getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			AnvizentAccumulator insertedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.INSERTED, true, new SQLBatchSinkStatsCalculator(StatsCategory.OUT, StatsNames.INSERTED));

			AnvizentAccumulator updatedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.UPDATED, true, new SQLBatchSinkStatsCalculator(StatsCategory.OUT, StatsNames.UPDATED));

			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), insertedAnvizentAccumulator, updatedAnvizentAccumulator);
		} else if (((RethinkDBSinkConfigBean) configBean).getDBInsertMode().equals(DBInsertMode.UPDATE)) {
			AnvizentAccumulator updatedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.UPDATED, true, new SQLBatchSinkStatsCalculator(StatsCategory.OUT, StatsNames.UPDATED));

			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), updatedAnvizentAccumulator);
		}
	}

	@Override
	public ResourceValidator getResourceConfigValidator() {
		// TODO Auto-generated method stub
		return null;
	}
}

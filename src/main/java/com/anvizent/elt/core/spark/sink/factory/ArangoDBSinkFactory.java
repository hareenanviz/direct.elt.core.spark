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
import com.anvizent.elt.core.spark.exception.TableDoesNotExistsException;
import com.anvizent.elt.core.spark.factory.RetryMandatoryFactory;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.sink.config.bean.ArangoDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.doc.helper.ArangoDBSinkDocHelper;
import com.anvizent.elt.core.spark.sink.function.ArangoDBSinkBatchFunction;
import com.anvizent.elt.core.spark.sink.function.ArangoDBSinkFunction;
import com.anvizent.elt.core.spark.sink.function.ArangoDBSinkInsertFunction;
import com.anvizent.elt.core.spark.sink.function.ArangoDBSinkInsertIfNotExistsFunction;
import com.anvizent.elt.core.spark.sink.function.ArangoDBSinkUpdateFunction;
import com.anvizent.elt.core.spark.sink.function.ArangoDBSinkUpsertFunction;
import com.anvizent.elt.core.spark.sink.service.ArangoDBSinkService;
import com.anvizent.elt.core.spark.sink.validator.ArangoDBSinkValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;
import com.arangodb.ArangoDBException;

/**
 * @author Hareen Bejjanki
 *
 */
public class ArangoDBSinkFactory extends SinkFactory implements RetryMandatoryFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected void write(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		ArangoDBSinkConfigBean arangoDBSinkConfigBean = (ArangoDBSinkConfigBean) configBean;

		try {
			if (!checkAndCreateTable(arangoDBSinkConfigBean)) {
				return;
			}

			LinkedHashMap<String, AnvizentDataType> newStructure = ArangoDBSinkService.getNewStructure(arangoDBSinkConfigBean, component.getStructure());

			ArangoDBSinkService.setSelectFields(arangoDBSinkConfigBean, component);
			AnvizentVoidFunction errorHandlerSinkFunction = ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink,
			        getName());
			JobDetails jobDetails = ErrorHandlerUtil.getJobDetails(configBean, getName());

			write(arangoDBSinkConfigBean, ApplicationBean.getInstance().getAccumulators(configBean.getName(), getName()), component, newStructure,
			        errorHandlerSinkFunction, jobDetails);
		} finally {
			ArangoDBSinkService.closeConnection(arangoDBSinkConfigBean.getConnection());
		}
	}

	private void write(ArangoDBSinkConfigBean arangoDBSinkConfigBean, ArrayList<AnvizentAccumulator> accumulators, Component component,
	        LinkedHashMap<String, AnvizentDataType> newStructure, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws UnimplementedException, InvalidRelationException, InvalidArgumentsException {
		if (arangoDBSinkConfigBean.getBatchType().equals(BatchType.NONE)) {
			recordProcessing(arangoDBSinkConfigBean, component, newStructure, accumulators, errorHandlerSinkFunction, jobDetails);
		} else {
			batchProcessing(arangoDBSinkConfigBean, component, newStructure, accumulators, errorHandlerSinkFunction, jobDetails);
		}
	}

	private void recordProcessing(ArangoDBSinkConfigBean arangoDBSinkConfigBean, Component component, LinkedHashMap<String, AnvizentDataType> newStructure,
	        ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws UnimplementedException, InvalidRelationException {
		ArangoDBSinkFunction arangoDBSinkFunction;

		if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT)) {
			arangoDBSinkFunction = new ArangoDBSinkInsertFunction(arangoDBSinkConfigBean, component.getStructure(), newStructure, accumulators,
			        errorHandlerSinkFunction, jobDetails);
		} else if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT_IF_NOT_EXISTS)) {
			arangoDBSinkFunction = new ArangoDBSinkInsertIfNotExistsFunction(arangoDBSinkConfigBean, component.getStructure(), newStructure, accumulators,
			        errorHandlerSinkFunction, jobDetails);
		} else if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)) {
			arangoDBSinkFunction = new ArangoDBSinkUpdateFunction(arangoDBSinkConfigBean, component.getStructure(), newStructure, accumulators,
			        errorHandlerSinkFunction, jobDetails);
		} else if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			arangoDBSinkFunction = new ArangoDBSinkUpsertFunction(arangoDBSinkConfigBean, component.getStructure(), newStructure, accumulators,
			        errorHandlerSinkFunction, jobDetails);
		} else {
			throw new UnimplementedException();
		}

		component.getRDD(arangoDBSinkConfigBean.getSourceStream()).foreach(arangoDBSinkFunction);
	}

	private void batchProcessing(ArangoDBSinkConfigBean arangoDBSinkConfigBean, Component component, LinkedHashMap<String, AnvizentDataType> newStructure,
	        ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws InvalidRelationException, InvalidArgumentsException {
		ArangoDBSinkBatchFunction arangoDBSinkBatchFunction = new ArangoDBSinkBatchFunction(arangoDBSinkConfigBean, component.getStructure(), newStructure,
		        accumulators, errorHandlerSinkFunction, jobDetails);
		component.getRDD(arangoDBSinkConfigBean.getSourceStream()).foreachPartition(arangoDBSinkBatchFunction);
	}

	private boolean checkAndCreateTable(ArangoDBSinkConfigBean arangoDBSinkConfigBean) throws Exception {
		boolean tableExists = ArangoDBSinkService.checkIfTableExists(arangoDBSinkConfigBean);

		if (arangoDBSinkConfigBean.getDBWriteMode().equals(DBWriteMode.APPEND)) {
			append(arangoDBSinkConfigBean, tableExists);
		} else if (arangoDBSinkConfigBean.getDBWriteMode().equals(DBWriteMode.IGNORE)) {
			return ignore(arangoDBSinkConfigBean, tableExists);
		} else if (arangoDBSinkConfigBean.getDBWriteMode().equals(DBWriteMode.OVERWRITE)) {
			overwrite(arangoDBSinkConfigBean, tableExists);
		} else if (arangoDBSinkConfigBean.getDBWriteMode().equals(DBWriteMode.TRUNCATE)) {
			truncate(arangoDBSinkConfigBean);
		} else {
			fail(arangoDBSinkConfigBean, tableExists);
		}

		return true;
	}

	private void append(ArangoDBSinkConfigBean arangoDBSinkConfigBean, boolean tableExists) throws Exception {
		if (!tableExists) {
			ArangoDBSinkService.createTable(arangoDBSinkConfigBean);
		}
	}

	private boolean ignore(ArangoDBSinkConfigBean arangoDBSinkConfigBean, boolean tableExists) throws Exception {
		if (tableExists) {
			return false;
		} else {
			ArangoDBSinkService.createTable(arangoDBSinkConfigBean);
			return true;
		}
	}

	private void overwrite(ArangoDBSinkConfigBean arangoDBSinkConfigBean, boolean tableExists) throws Exception {
		if (tableExists) {
			ArangoDBSinkService.dropTable(arangoDBSinkConfigBean);
		}

		ArangoDBSinkService.createTable(arangoDBSinkConfigBean);
	}

	private void truncate(ArangoDBSinkConfigBean arangoDBSinkConfigBean) throws Exception {
		try {
			ArangoDBSinkService.truncate(arangoDBSinkConfigBean);
		} catch (ArangoDBException arangoDBException) {
			if (arangoDBException.getErrorNum() == 1203 && arangoDBException.getErrorMessage().contains("collection not found")) {
				throw new TableDoesNotExistsException(
				        "Table '" + arangoDBSinkConfigBean.getTableName() + "' does not exists for truncate. Details: " + arangoDBException.getErrorMessage(),
				        arangoDBException);
			}
		}
	}

	private void fail(ArangoDBSinkConfigBean arangoDBSinkConfigBean, boolean tableExists) throws Exception {
		if (tableExists) {
			throw new TableAlreadyExistsException("Table '" + arangoDBSinkConfigBean.getTableName() + "' already exists");
		} else {
			ArangoDBSinkService.createTable(arangoDBSinkConfigBean);
		}
	}

	@Override
	public String getName() {
		return Components.ARANGO_DB_SINK.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new ArangoDBSinkDocHelper(this);
	}

	@Override
	public Validator getValidator() {
		return new ArangoDBSinkValidator(this);
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
		ArangoDBSinkConfigBean arangoDBSinkConfigBean = (ArangoDBSinkConfigBean) configAndMappingConfigBeans.getConfigBean();

		if (arangoDBSinkConfigBean.getBatchType().equals(BatchType.NONE)) {
			super.createAccumulators(configAndMappingConfigBeans, globalStatsType);
		} else {
			createBatchAccumulators(configAndMappingConfigBeans, globalStatsType);
		}
	}

	@Override
	protected void createSpecialAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		if (((ArangoDBSinkConfigBean) configBean).getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			AnvizentAccumulator insertedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.INSERTED, true, new SQLSinkStatsCalculator(StatsCategory.OUT, StatsNames.INSERTED));

			AnvizentAccumulator updatedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.UPDATED, true, new SQLSinkStatsCalculator(StatsCategory.OUT, StatsNames.UPDATED));

			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), insertedAnvizentAccumulator, updatedAnvizentAccumulator);
		} else if (((ArangoDBSinkConfigBean) configBean).getDBInsertMode().equals(DBInsertMode.UPDATE)) {
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

		if (((ArangoDBSinkConfigBean) configBean).getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			AnvizentAccumulator insertedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.INSERTED, true, new SQLBatchSinkStatsCalculator(StatsCategory.OUT, StatsNames.INSERTED));

			AnvizentAccumulator updatedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.UPDATED, true, new SQLBatchSinkStatsCalculator(StatsCategory.OUT, StatsNames.UPDATED));

			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), insertedAnvizentAccumulator, updatedAnvizentAccumulator);
		} else if (((ArangoDBSinkConfigBean) configBean).getDBInsertMode().equals(DBInsertMode.UPDATE)) {
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

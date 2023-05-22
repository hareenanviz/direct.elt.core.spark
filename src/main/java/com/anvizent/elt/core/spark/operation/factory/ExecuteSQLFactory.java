package com.anvizent.elt.core.spark.operation.factory;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentFromRowStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentToRowStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Components;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.operation.config.bean.ExecuteSQLConfigBean;
import com.anvizent.elt.core.spark.operation.doc.helper.ExecuteSQLDocHelper;
import com.anvizent.elt.core.spark.operation.validator.ExecuteSQLValidator;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class ExecuteSQLFactory extends MultiInputOperationFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected Component process(ConfigBean configBean, LinkedHashMap<String, Component> components, ErrorHandlerSink errorHandlerSink) throws Exception {
		ExecuteSQLConfigBean sqlExecutorConfigBean = (ExecuteSQLConfigBean) configBean;

		createOrReplaceTempViews(components, sqlExecutorConfigBean.getSources(), sqlExecutorConfigBean, errorHandlerSink);
		Dataset<Row> resultingDataset = components.get(sqlExecutorConfigBean.getSources().get(0)).getSparkSession().sql(sqlExecutorConfigBean.getQuery());

		return Component.createComponent(components.get(sqlExecutorConfigBean.getSources().get(0)).getSparkSession(), sqlExecutorConfigBean.getName(),
		        resultingDataset, resultingDataset.schema(), configBean,
		        ApplicationBean.getInstance().getAccumulators(configBean.getName(), getName() + " From Row"),
		        ErrorHandlerUtil.getErrorHandlerFunction(configBean, components.get(sqlExecutorConfigBean.getSources().get(0)).getStructure(), errorHandlerSink,
		                getName() + " From Row"),
		        ErrorHandlerUtil.getJobDetails(sqlExecutorConfigBean, getName() + " From Row"));
	}

	private void createOrReplaceTempViews(LinkedHashMap<String, Component> components, ArrayList<String> inputSources,
	        ExecuteSQLConfigBean sqlExecutorConfigBean, ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		ArrayList<String> sourceAliases = sqlExecutorConfigBean.getSourceAliaseNames();
		InvalidConfigException invalidConfigException = new InvalidConfigException();

		invalidConfigException.setComponent(sqlExecutorConfigBean.getConfigName());
		invalidConfigException.setComponentName(sqlExecutorConfigBean.getName());
		invalidConfigException.setSeekDetails(sqlExecutorConfigBean.getSeekDetails());

		for (int i = 0; i < inputSources.size(); i++) {
			if (components.get(inputSources.get(i)) == null) {
				invalidConfigException.add(General.SOURCE + ": " + inputSources.get(i) + " is not found!");
			}

			if (invalidConfigException.getNumberOfExceptions() == 0) {
				components.get(inputSources.get(i))
				        .getRDDAsDataset(sqlExecutorConfigBean.getSourceStream(), sqlExecutorConfigBean,
				                ApplicationBean.getInstance().getAccumulators(sqlExecutorConfigBean.getName(), getName() + " " + inputSources.get(i)),
				                ErrorHandlerUtil.getErrorHandlerFunction(sqlExecutorConfigBean, components.get(inputSources.get(i)).getStructure(),
				                        errorHandlerSink, getName() + " " + inputSources.get(i)),
				                ErrorHandlerUtil.getJobDetails(sqlExecutorConfigBean, getName() + " " + inputSources.get(i)))
				        .createOrReplaceTempView(sourceAliases.get(i));
			}
		}
	}

	@Override
	public String getName() {
		return Components.EXECUTE_SQL.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new ExecuteSQLDocHelper(this);
	}

	@Override
	public Validator getValidator() {
		return new ExecuteSQLValidator(this);
	}

	@Override
	public SchemaValidator getSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getMaxInputs() {
		return null;
	}

	@Override
	public Integer getMinInputs() {
		return 1;
	}

	@Override
	public void createAccumulators(ConfigAndMappingConfigBeans configAndMappingConfigBeans, StatsType globalStatsType) {
		StatsType statsType = getStatsType(configAndMappingConfigBeans.getStatsStore(), globalStatsType);

		if (statsType.equals(StatsType.NONE)) {
			return;
		}

		boolean componentLevel = getNotEmptyMappingConfigBeans(configAndMappingConfigBeans.getMappingConfigBeans()) > 0 ? true : false;
		createFactoryAccumulators(configAndMappingConfigBeans.getConfigBean(), statsType, componentLevel);
		createSpecialAccumulators(configAndMappingConfigBeans.getConfigBean(), statsType, componentLevel);
	}

	@Override
	protected IStatsCalculator getStatsCalculator(StatsCategory statsCategory, String statsName, String internalRDDName) {
		if (internalRDDName.equals(getName() + " From Row")) {
			return new AnvizentFromRowStatsCalculator(statsCategory, statsName);
		} else {
			return new AnvizentToRowStatsCalculator(statsCategory, statsName);
		}
	}

	@Override
	protected void createFactoryAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		ExecuteSQLConfigBean executeSQLConfigBean = (ExecuteSQLConfigBean) configBean;

		for (int i = 0; i < executeSQLConfigBean.getSources().size(); i++) {
			createToRowFactoryAccumulators(executeSQLConfigBean, executeSQLConfigBean.getSources().get(i), statsType, componentLevel);
		}

		createFromRowFactoryAccumulators(configBean, statsType, componentLevel);
	}

	private void createToRowFactoryAccumulators(ExecuteSQLConfigBean executeSQLConfigBean, String sourceName, StatsType statsType, boolean componentLevel) {
		AnvizentAccumulator inAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), executeSQLConfigBean.getName(),
		        getName() + " " + sourceName, StatsCategory.IN, StatsNames.IN, true,
		        getStatsCalculator(StatsCategory.IN, StatsNames.IN, getName() + " " + sourceName));
		AnvizentAccumulator errorToRowAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(),
		        executeSQLConfigBean.getName(), getName() + " " + sourceName, StatsCategory.ERROR, StatsNames.ERROR, true,
		        getStatsCalculator(StatsCategory.ERROR, StatsNames.ERROR, getName() + " " + sourceName));

		ApplicationBean.getInstance().addAccumulator(executeSQLConfigBean.getName(), getName() + " " + sourceName, inAnvizentAccumulator,
		        errorToRowAnvizentAccumulator);
	}

	private void createFromRowFactoryAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		if ((statsType.equals(StatsType.COMPONENT_LEVEL) && !componentLevel) || statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator outAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName() + " From Row", StatsCategory.OUT, StatsNames.OUT, !componentLevel,
			        getStatsCalculator(StatsCategory.OUT, StatsNames.OUT, getName() + " From Row"));

			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName() + " From Row", outAnvizentAccumulator);
		}

		AnvizentAccumulator errorFromRowAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        getName() + " From Row", StatsCategory.ERROR, StatsNames.ERROR, true,
		        getStatsCalculator(StatsCategory.ERROR, StatsNames.ERROR, getName() + " From Row"));

		ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName() + " From Row", errorFromRowAnvizentAccumulator);
	}

	@Override
	protected void createSpecialAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		// TODO Auto-generated method stub
	}

	@Override
	public ResourceValidator getResourceConfigValidator() {
		// TODO Auto-generated method stub
		return null;
	}
}

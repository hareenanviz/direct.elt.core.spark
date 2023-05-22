package com.anvizent.elt.core.spark.filter.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

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
import com.anvizent.elt.core.lib.stats.calculator.AnvizentFilterStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Filter.Components;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.filter.config.bean.FilterByExpressionConfigBean;
import com.anvizent.elt.core.spark.filter.doc.helper.FilterByExpressionDocHelper;
import com.anvizent.elt.core.spark.filter.function.FilterByExpressionFunction;
import com.anvizent.elt.core.spark.filter.validator.FilterByExpressionValidator;
import com.anvizent.elt.core.spark.operation.factory.SimpleOperationFactory;
import com.anvizent.elt.core.spark.operation.service.ExpressionService;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FilterByExpressionFactory extends SimpleOperationFactory {
	private static final long serialVersionUID = 1L;

	@Override
	protected Component process(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		FilterByExpressionConfigBean filterByExpConfigBean = (FilterByExpressionConfigBean) configBean;
		ExpressionService.decodeIfElseExpressions(filterByExpConfigBean);

		LinkedHashMap<String, AnvizentDataType> newStructure = new LinkedHashMap<String, AnvizentDataType>(component.getStructure());

		JavaRDD<HashMap<String, Object>> filteredRDD = getFilteredRDD(0, filterByExpConfigBean, component, newStructure,
		        getAccumulators(0, filterByExpConfigBean), getErrorHandlerSinkFunction(0, filterByExpConfigBean, component, errorHandlerSink),
		        getJobDetails(0, filterByExpConfigBean));

		Component finalComponent = Component.createComponent(component.getSparkSession(), filterByExpConfigBean.getName(),
		        filterByExpConfigBean.getEmitStreamNames().get(0), filteredRDD, newStructure);

		for (int i = 1; i < filterByExpConfigBean.getExpressions().size(); i++) {
			finalComponent.addRDD(filterByExpConfigBean.getEmitStreamNames().get(i),
			        getFilteredRDD(i, filterByExpConfigBean, component, newStructure, getAccumulators(i, filterByExpConfigBean),
			                getErrorHandlerSinkFunction(i, filterByExpConfigBean, component, errorHandlerSink), getJobDetails(i, filterByExpConfigBean)));
		}

		return finalComponent;
	}

	private AnvizentVoidFunction getErrorHandlerSinkFunction(int index, FilterByExpressionConfigBean filterByExpConfigBean, Component component,
	        ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		AnvizentVoidFunction errorHandlerSinkFunction = ErrorHandlerUtil.getErrorHandlerFunction(filterByExpConfigBean, component.getStructure(),
		        errorHandlerSink, getName() + "_" + filterByExpConfigBean.getEmitStreamNames().get(index));

		return errorHandlerSinkFunction;
	}

	private JobDetails getJobDetails(int index, FilterByExpressionConfigBean filterByExpConfigBean)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		JobDetails jobDetails = ErrorHandlerUtil.getJobDetails(filterByExpConfigBean, getName() + "_" + filterByExpConfigBean.getEmitStreamNames().get(index));

		return jobDetails;
	}

	private ArrayList<AnvizentAccumulator> getAccumulators(int index, FilterByExpressionConfigBean filterByExpConfigBean) {
		ArrayList<AnvizentAccumulator> accumulators = new ArrayList<>();

		if (ApplicationBean.getInstance().getAccumulators(filterByExpConfigBean.getName(), getName()) != null) {
			accumulators.addAll(ApplicationBean.getInstance().getAccumulators(filterByExpConfigBean.getName(), getName()));
		}

		if (ApplicationBean.getInstance().getAccumulators(filterByExpConfigBean.getName(),
		        getName() + "_" + filterByExpConfigBean.getEmitStreamNames().get(index)) != null) {
			accumulators.addAll(ApplicationBean.getInstance().getAccumulators(filterByExpConfigBean.getName(),
			        getName() + "_" + filterByExpConfigBean.getEmitStreamNames().get(index)));
		}

		return accumulators;
	}

	private JavaRDD<HashMap<String, Object>> getFilteredRDD(int i, FilterByExpressionConfigBean filterByExpConfigBean, Component component,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws InvalidArgumentsException {
		JavaRDD<HashMap<String, Object>> filteredRDD = component.getRDD(filterByExpConfigBean.getSourceStream()).filter(new FilterByExpressionFunction(
		        filterByExpConfigBean, i, component.getStructure(), newStructure, accumulators, errorHandlerSinkFunction, jobDetails));

		if (filterByExpConfigBean.isPersist()) {
			filteredRDD.persist(StorageLevel.MEMORY_AND_DISK());
		}

		return filteredRDD;
	}

	@Override
	public String getName() {
		return Components.FILTER_BY_EXPRESSION.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new FilterByExpressionDocHelper(this);
	}

	@Override
	public Validator getValidator() {
		return new FilterByExpressionValidator(this);
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
	protected void createFactoryAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		FilterByExpressionConfigBean filterByExpConfigBean = (FilterByExpressionConfigBean) configBean;

		AnvizentAccumulator inAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), filterByExpConfigBean.getName(),
		        getName(), StatsCategory.IN, StatsNames.IN, true, getStatsCalculator(StatsCategory.IN, StatsNames.IN, getName()));
		ApplicationBean.getInstance().addAccumulator(filterByExpConfigBean.getName(), getName(), inAnvizentAccumulator);

		for (int i = 0; i < filterByExpConfigBean.getEmitStreamNames().size(); i++) {
			String internalRDDName = getName() + "_" + filterByExpConfigBean.getEmitStreamNames().get(i);
			createFactoryAccumulators(filterByExpConfigBean.getName(), internalRDDName, statsType, componentLevel);
		}
	}

	@Override
	protected IStatsCalculator getStatsCalculator(StatsCategory statsCategory, String statsName, String internalRDDName) {
		return new AnvizentFilterStatsCalculator(statsCategory, statsName);
	}

	private void createFactoryAccumulators(String componentName, String internalRDDName, StatsType statsType, boolean componentLevel) {
		AnvizentAccumulator filteredAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), componentName,
		        internalRDDName, StatsCategory.OTHER, StatsNames.FILTERED, true, getStatsCalculator(StatsCategory.OTHER, StatsNames.FILTERED, internalRDDName));
		AnvizentAccumulator errorAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), componentName, internalRDDName,
		        StatsCategory.ERROR, StatsNames.ERROR, true, getStatsCalculator(StatsCategory.ERROR, StatsNames.ERROR, internalRDDName));

		ApplicationBean.getInstance().addAccumulator(componentName, internalRDDName, filteredAnvizentAccumulator, errorAnvizentAccumulator);

		if ((statsType.equals(StatsType.COMPONENT_LEVEL) && !componentLevel) || statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator outAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), componentName,
			        internalRDDName, StatsCategory.OUT, StatsNames.OUT, !componentLevel,
			        getStatsCalculator(StatsCategory.OUT, StatsNames.OUT, internalRDDName));

			ApplicationBean.getInstance().addAccumulator(componentName, internalRDDName, outAnvizentAccumulator);
		}
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
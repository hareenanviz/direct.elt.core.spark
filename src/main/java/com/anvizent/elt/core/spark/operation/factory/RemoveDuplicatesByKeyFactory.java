package com.anvizent.elt.core.spark.operation.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.stats.calculator.Anvizent2StatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentMapToPairStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.RDDSpecialNames;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Components;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.operation.config.bean.RemoveDuplicatesByKeyConfigBean;
import com.anvizent.elt.core.spark.operation.doc.helper.RemoveDuplicatesByKeyDocHelper;
import com.anvizent.elt.core.spark.operation.function.PairToNormalRDDListFunction;
import com.anvizent.elt.core.spark.operation.function.RemoveDuplicatesByKeyPairFunction;
import com.anvizent.elt.core.spark.operation.function.RemoveDuplicatesByKeyReduceFunction;
import com.anvizent.elt.core.spark.operation.validator.RemoveDuplicatesByKeyValidator;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class RemoveDuplicatesByKeyFactory extends SimpleOperationFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected Component process(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		RemoveDuplicatesByKeyConfigBean removeDuplicatesByKeyConfigBean = (RemoveDuplicatesByKeyConfigBean) configBean;

		JavaPairRDD<ArrayList<Object>, HashMap<String, Object>> pairedRDD = component.getRDD(removeDuplicatesByKeyConfigBean.getSourceStream())
		        .mapToPair(new RemoveDuplicatesByKeyPairFunction(removeDuplicatesByKeyConfigBean, component.getStructure(),
		                ApplicationBean.getInstance().getAccumulators(removeDuplicatesByKeyConfigBean.getName(), getName() + " " + RDDSpecialNames.PAIR),
		                ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink,
		                        getName() + " " + RDDSpecialNames.PAIR),
		                ErrorHandlerUtil.getJobDetails(configBean, getName() + " " + RDDSpecialNames.PAIR)));

		JavaPairRDD<ArrayList<Object>, HashMap<String, Object>> reducedRDD = pairedRDD
		        .reduceByKey(new RemoveDuplicatesByKeyReduceFunction(removeDuplicatesByKeyConfigBean, component.getStructure(),
		                ApplicationBean.getInstance().getAccumulators(removeDuplicatesByKeyConfigBean.getName(), getName() + " " + RDDSpecialNames.REDUCE),
		                ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink,
		                        getName() + " " + RDDSpecialNames.REDUCE),
		                ErrorHandlerUtil.getJobDetails(configBean, getName() + " " + RDDSpecialNames.REDUCE)));

		JavaRDD<HashMap<String, Object>> finalRDD = reducedRDD
		        .flatMap(new PairToNormalRDDListFunction(removeDuplicatesByKeyConfigBean, component.getStructure(), component.getStructure(),
		                ApplicationBean.getInstance().getAccumulators(removeDuplicatesByKeyConfigBean.getName(), getName()),
		                ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink, getName()),
		                ErrorHandlerUtil.getJobDetails(configBean, getName())));

		return Component.createComponent(ApplicationBean.getInstance().getSparkSession(), removeDuplicatesByKeyConfigBean.getName(), finalRDD,
		        component.getStructure());
	}

	@Override
	public String getName() {
		return Components.REMOVE_DUPLICATES_BY_KEY.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new RemoveDuplicatesByKeyDocHelper(this);
	}

	@Override
	public Validator getValidator() {
		return new RemoveDuplicatesByKeyValidator(this);
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
		if (internalRDDName.equals(getName() + " " + RDDSpecialNames.PAIR)) {
			return new AnvizentMapToPairStatsCalculator<LinkedHashMap<String, Object>, ArrayList<Object>>(statsCategory, statsName);
		} else if (internalRDDName.equals(getName() + " " + RDDSpecialNames.REDUCE)) {
			return new Anvizent2StatsCalculator<LinkedHashMap<String, Object>>(statsCategory, statsName);
		} else {
			return new AnvizentStatsCalculator<LinkedHashMap<String, Object>, LinkedHashMap<String, Object>>(statsCategory, statsName);
		}
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
		createPairRDDAccumulators(configBean, statsType, componentLevel);
		createReduceRDDAccumulators(configBean, statsType, componentLevel);
		createOutAccumulators(configBean, statsType, componentLevel);
	}

	private void createPairRDDAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		AnvizentAccumulator inAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        getName() + " " + RDDSpecialNames.PAIR, StatsCategory.IN, StatsNames.IN, true,
		        getStatsCalculator(StatsCategory.IN, StatsNames.IN, getName() + " " + RDDSpecialNames.PAIR));
		AnvizentAccumulator errorAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        getName() + " " + RDDSpecialNames.PAIR, StatsCategory.ERROR, StatsNames.ERROR, true,
		        getStatsCalculator(StatsCategory.ERROR, StatsNames.ERROR, getName() + " " + RDDSpecialNames.PAIR));

		ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName() + " " + RDDSpecialNames.PAIR, inAnvizentAccumulator,
		        errorAnvizentAccumulator);
	}

	private void createReduceRDDAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		AnvizentAccumulator removedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        getName() + " " + RDDSpecialNames.REDUCE, StatsCategory.OTHER, StatsNames.REMOVED, true,
		        getStatsCalculator(StatsCategory.OTHER, StatsNames.REMOVED, getName() + " " + RDDSpecialNames.REDUCE));
		AnvizentAccumulator errorAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        getName() + " " + RDDSpecialNames.REDUCE, StatsCategory.ERROR, StatsNames.ERROR, true,
		        getStatsCalculator(StatsCategory.ERROR, StatsNames.ERROR, getName() + " " + RDDSpecialNames.REDUCE));

		ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName() + " " + RDDSpecialNames.REDUCE, removedAnvizentAccumulator,
		        errorAnvizentAccumulator);
	}

	private void createOutAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		if ((statsType.equals(StatsType.COMPONENT_LEVEL) && !componentLevel) || statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator outAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.OUT, !componentLevel, getStatsCalculator(StatsCategory.OUT, StatsNames.OUT, getName()));

			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), outAnvizentAccumulator);
		}

		AnvizentAccumulator errorAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(), getName(),
		        StatsCategory.ERROR, StatsNames.ERROR, true, getStatsCalculator(StatsCategory.ERROR, StatsNames.ERROR, getName()));

		ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), errorAnvizentAccumulator);
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

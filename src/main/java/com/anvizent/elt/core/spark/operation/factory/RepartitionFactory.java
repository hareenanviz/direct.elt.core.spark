package com.anvizent.elt.core.spark.operation.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentMapToPairStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.RDDSpecialNames;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Components;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.operation.config.bean.RepartitionConfigBean;
import com.anvizent.elt.core.spark.operation.doc.helper.RepartitionDocHelper;
import com.anvizent.elt.core.spark.operation.function.PairToNormalRDDListFunction;
import com.anvizent.elt.core.spark.operation.function.RepartitionPairFunction;
import com.anvizent.elt.core.spark.operation.validator.RepartitionValidator;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class RepartitionFactory extends SimpleOperationFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected Component process(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		RepartitionConfigBean repartitionConfigBean = (RepartitionConfigBean) configBean;

		JavaRDD<HashMap<String, Object>> sourceRDD = component.getRDD(repartitionConfigBean.getSourceStream());

		Integer numPartitions = getNumberOfPartitions(repartitionConfigBean, sourceRDD);

		JavaRDD<HashMap<String, Object>> finalRDD = repartition(repartitionConfigBean, component, numPartitions, sourceRDD, errorHandlerSink);

		return Component.createComponent(ApplicationBean.getInstance().getSparkSession(), repartitionConfigBean.getName(), finalRDD, component.getStructure());
	}

	private JavaRDD<HashMap<String, Object>> repartition(RepartitionConfigBean repartitionConfigBean, Component component, Integer numPartitions,
	        JavaRDD<HashMap<String, Object>> sourceRDD, ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		if (repartitionConfigBean.getKeyFields() != null && !repartitionConfigBean.getKeyFields().isEmpty()) {
			verifyKeyField(repartitionConfigBean, component.getStructure());

			JavaPairRDD<ArrayList<Object>, HashMap<String, Object>> pairRDD = sourceRDD
			        .mapToPair(new RepartitionPairFunction(repartitionConfigBean, component.getStructure(),
			                ApplicationBean.getInstance().getAccumulators(repartitionConfigBean.getName(), getName() + " " + RDDSpecialNames.PAIR),
			                ErrorHandlerUtil.getErrorHandlerFunction(repartitionConfigBean, component.getStructure(), errorHandlerSink,
			                        getName() + " " + RDDSpecialNames.PAIR),
			                ErrorHandlerUtil.getJobDetails(repartitionConfigBean, getName() + " " + RDDSpecialNames.PAIR)));

			pairRDD = pairRDD.partitionBy(new HashPartitioner(numPartitions));

			return pairRDD.flatMap(new PairToNormalRDDListFunction(repartitionConfigBean, component.getStructure(), component.getStructure(),
			        ApplicationBean.getInstance().getAccumulators(repartitionConfigBean.getName(), getName()),
			        ErrorHandlerUtil.getErrorHandlerFunction(repartitionConfigBean, component.getStructure(), errorHandlerSink, getName()),
			        ErrorHandlerUtil.getJobDetails(repartitionConfigBean, getName())));
		} else if (numPartitions > sourceRDD.getNumPartitions()) {
			return sourceRDD.repartition(numPartitions);
		} else if (numPartitions < sourceRDD.getNumPartitions()) {
			return sourceRDD.coalesce(numPartitions);
		} else {
			return sourceRDD;
		}
	}

	private void verifyKeyField(RepartitionConfigBean repartitionConfigBean, LinkedHashMap<String, AnvizentDataType> structure) throws InvalidConfigException {
		for (String keyField : repartitionConfigBean.getKeyFields()) {
			if (!structure.containsKey(keyField)) {

				InvalidConfigException invalidConfigException = new InvalidConfigException();

				invalidConfigException.setComponent(repartitionConfigBean.getConfigName());
				invalidConfigException.setComponentName(repartitionConfigBean.getName());
				invalidConfigException.setSeekDetails(repartitionConfigBean.getSeekDetails());

				invalidConfigException.add("Provided repartition column '" + keyField + "' is not present in source.");

				throw invalidConfigException;
			}
		}
	}

	private Integer getNumberOfPartitions(RepartitionConfigBean repartitionConfigBean, JavaRDD<HashMap<String, Object>> javaRDD) {
		Integer numPartitions = repartitionConfigBean.getNumberOfPartitions();

		if (numPartitions == null) {
			numPartitions = javaRDD.getNumPartitions();
		}

		return numPartitions;
	}

	@Override
	public String getName() {
		return Components.REPARTITION.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new RepartitionDocHelper(this);
	}

	@Override
	public Validator getValidator() {
		return new RepartitionValidator(this);
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
		RepartitionConfigBean repartitionConfigBean = (RepartitionConfigBean) configAndMappingConfigBeans.getConfigBean();

		if (repartitionConfigBean.getKeyFields() != null && !repartitionConfigBean.getKeyFields().isEmpty()) {
			createFactoryAccumulators(configAndMappingConfigBeans.getConfigBean(), statsType, componentLevel);
			createSpecialAccumulators(configAndMappingConfigBeans.getConfigBean(), statsType, componentLevel);
		}
	}

	@Override
	protected void createFactoryAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		createPairRDDAccumulators(configBean, statsType, componentLevel);
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

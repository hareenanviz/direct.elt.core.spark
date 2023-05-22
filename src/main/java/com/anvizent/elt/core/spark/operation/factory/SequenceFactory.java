package com.anvizent.elt.core.spark.operation.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Components;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.operation.config.bean.SequenceConfigBean;
import com.anvizent.elt.core.spark.operation.doc.helper.SequenceDocHelper;
import com.anvizent.elt.core.spark.operation.function.SequenceFunction;
import com.anvizent.elt.core.spark.operation.validator.SequenceValidator;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.util.StructureUtil;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

import scala.Tuple2;

/**
 * @author Hareen Bejjanki
 *
 */
public class SequenceFactory extends SimpleOperationFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected Component process(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		SequenceConfigBean sequenceConfigBean = (SequenceConfigBean) configBean;

		LinkedHashMap<String, AnvizentDataType> newStructure = getNewStructure(component.getStructure(), sequenceConfigBean);
		JavaPairRDD<HashMap<String, Object>, Long> indexedPairRDD = component.getRDD(configBean.getSourceStream()).zipWithIndex();
		JavaRDD<HashMap<String, Object>> javaRDD = indexedPairRDD.flatMap(new SequenceFunction(sequenceConfigBean, component.getStructure(), newStructure,
		        ApplicationBean.getInstance().getAccumulators(sequenceConfigBean.getName(), getName()),
		        ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink, getName()),
		        ErrorHandlerUtil.getJobDetails(sequenceConfigBean, getName())));

		return Component.createComponent(component.getSparkSession(), configBean.getName(), javaRDD, newStructure);
	}

	private LinkedHashMap<String, AnvizentDataType> getNewStructure(LinkedHashMap<String, AnvizentDataType> structure, SequenceConfigBean sequenceConfigBean)
	        throws UnsupportedException, InvalidConfigException {
		ArrayList<AnvizentDataType> anvizentDataTypes = new ArrayList<>();
		for (int i = 0; i < sequenceConfigBean.getFields().size(); i++) {
			anvizentDataTypes.add(new AnvizentDataType(Long.class));
		}

		return StructureUtil.getNewStructure(sequenceConfigBean, structure, sequenceConfigBean.getFields(), anvizentDataTypes,
		        sequenceConfigBean.getFieldIndexes(), General.FIELDS, General.FIELD_INDEXES);
	}

	@Override
	public String getName() {
		return Components.SEQUENCE.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new SequenceDocHelper(this);
	}

	@Override
	public Validator getValidator() {
		return new SequenceValidator(this);
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
		return new AnvizentStatsCalculator<Tuple2<LinkedHashMap<String, Object>, Long>, LinkedHashMap<String, Object>>(statsCategory, statsName);
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
		// TODO Auto-generated method stub
	}

	@Override
	public ResourceValidator getResourceConfigValidator() {
		// TODO Auto-generated method stub
		return null;
	}
}

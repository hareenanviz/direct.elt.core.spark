package com.anvizent.elt.core.spark.operation.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

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
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Components;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.operation.config.bean.ExpressionConfigBean;
import com.anvizent.elt.core.spark.operation.doc.helper.ExpressionDocHelper;
import com.anvizent.elt.core.spark.operation.function.ExpressionFunction;
import com.anvizent.elt.core.spark.operation.service.ExpressionService;
import com.anvizent.elt.core.spark.operation.validator.ExpressionValidator;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.util.StructureUtil;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class ExpressionFactory extends SimpleOperationFactory {
	private static final long serialVersionUID = 1L;

	@Override
	protected Component process(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		ExpressionConfigBean expressionConfigBean = (ExpressionConfigBean) configBean;
		ExpressionService.decodeExpressions(expressionConfigBean);
		LinkedHashMap<String, AnvizentDataType> newStructure = getNewStructure(expressionConfigBean, component.getStructure());

		JavaRDD<HashMap<String, Object>> expressionRDD = component.getRDD(expressionConfigBean.getSourceStream())
		        .flatMap(new ExpressionFunction(expressionConfigBean, component.getStructure(), newStructure,
		                ApplicationBean.getInstance().getAccumulators(expressionConfigBean.getName(), getName()),
		                ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink, getName()),
		                ErrorHandlerUtil.getJobDetails(configBean, getName())));

		if (configBean.isPersist()) {
			expressionRDD.persist(StorageLevel.MEMORY_AND_DISK());
		}

		return Component.createComponent(component.getSparkSession(), expressionConfigBean.getName(), expressionRDD, newStructure);
	}

	private LinkedHashMap<String, AnvizentDataType> getNewStructure(ExpressionConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure)
	        throws UnsupportedException, InvalidConfigException {

		LinkedHashMap<String, AnvizentDataType> newStructure = new LinkedHashMap<String, AnvizentDataType>(structure);

		ArrayList<AnvizentDataType> newDataTypes = StructureUtil.getNewStructureDataTypes(configBean.getReturnTypes(), configBean.getPrecisions(),
		        configBean.getScales());

		return StructureUtil.getNewStructure(configBean, newStructure, configBean.getExpressionsFieldNames(), newDataTypes,
		        configBean.getExpressionsFieldIndexes(), Operation.Expression.EXPRESSIONS_FIELD_NAMES, Operation.Expression.EXPRESSIONS_FIELD_NAMES_INDEXES);
	}

	@Override
	public String getName() {
		return Components.EXPRESSION.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new ExpressionDocHelper(this);
	}

	@Override
	public Validator getValidator() {
		return new ExpressionValidator(this);
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
		return new AnvizentStatsCalculator<LinkedHashMap<String, Object>, LinkedHashMap<String, Object>>(statsCategory, statsName);
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

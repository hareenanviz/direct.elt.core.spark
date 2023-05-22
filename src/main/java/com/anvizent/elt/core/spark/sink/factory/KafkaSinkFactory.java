package com.anvizent.elt.core.spark.sink.factory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.function.ConvertToRowFunction;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentToRowStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.RDDSpecialNames;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink.Components;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.constant.SparkConstants;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.RetryMandatoryFactory;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.sink.config.bean.KafkaSinkConfigBean;
import com.anvizent.elt.core.spark.sink.doc.helper.KafkaSinkDocHelper;
import com.anvizent.elt.core.spark.sink.function.KafkaSinkFunction;
import com.anvizent.elt.core.spark.sink.validator.KafkaSinkValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class KafkaSinkFactory extends SinkFactory implements RetryMandatoryFactory {
	private static final long serialVersionUID = 1L;

	@Override
	public void write(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		KafkaSinkConfigBean kafkaSinkConfigBean = (KafkaSinkConfigBean) configBean;

		validate(kafkaSinkConfigBean, component.getStructure());

		LinkedHashMap<String, AnvizentDataType> newStructure = getNewStructure(kafkaSinkConfigBean, component.getStructure());

		JavaRDD<HashMap<String, Object>> kafkaRdd = component.getRDD(kafkaSinkConfigBean.getSourceStream())
		        .flatMap(new KafkaSinkFunction(kafkaSinkConfigBean, component.getStructure(), newStructure,
		                ApplicationBean.getInstance().getAccumulators(kafkaSinkConfigBean.getName(), getName()),
		                ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink, getName()),
		                ErrorHandlerUtil.getJobDetails(configBean, getName())));

		if (configBean.isPersist()) {
			kafkaRdd.persist(StorageLevel.MEMORY_AND_DISK());
		}

		JavaRDD<Row> kafkaRowRDD = kafkaRdd.flatMap(new ConvertToRowFunction(kafkaSinkConfigBean, newStructure,
		        ApplicationBean.getInstance().getAccumulators(kafkaSinkConfigBean.getName(), getName() + " " + RDDSpecialNames.TO_ROW),
		        ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink, getName() + " " + RDDSpecialNames.TO_ROW),
		        ErrorHandlerUtil.getJobDetails(configBean, getName() + " " + RDDSpecialNames.TO_ROW)));

		if (configBean.isPersist()) {
			kafkaRowRDD.persist(StorageLevel.MEMORY_AND_DISK());
		}

		Dataset<Row> dataset = ApplicationBean.getInstance().getSparkSession().createDataFrame(kafkaRowRDD, Component.getStructType(newStructure));

		if (configBean.isPersist()) {
			dataset.persist(StorageLevel.MEMORY_AND_DISK());
		}

		dataset.write().format("kafka").options(getOptions(kafkaSinkConfigBean)).save();
	}

	private void validate(KafkaSinkConfigBean kafkaSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure) {
		InvalidConfigException exception = new InvalidConfigException();
		exception.setComponent(kafkaSinkConfigBean.getConfigName());
		exception.setComponentName(kafkaSinkConfigBean.getName());
		exception.setSeekDetails(kafkaSinkConfigBean.getSeekDetails());

		if (kafkaSinkConfigBean.getTopicField() != null && !structure.containsKey(kafkaSinkConfigBean.getTopicField())) {
			exception.add(Message.IS_NOT_INT, kafkaSinkConfigBean.getTopicField(), "structure");
		}

		if (kafkaSinkConfigBean.getKeyField() != null && !structure.containsKey(kafkaSinkConfigBean.getKeyField())) {
			exception.add(Message.IS_NOT_INT, kafkaSinkConfigBean.getKeyField(), "structure");
		}
	}

	private Map<String, String> getOptions(KafkaSinkConfigBean expressionConfigBean) {
		expressionConfigBean.getConfig().put("acks", "all");
		expressionConfigBean.getConfig().put("client.id", "ELT_CORE_" + expressionConfigBean.getName());
		expressionConfigBean.getConfig().put("enable.idempotence", "true");

		return expressionConfigBean.getConfig();
	}

	private LinkedHashMap<String, AnvizentDataType> getNewStructure(KafkaSinkConfigBean expressionConfigBean, LinkedHashMap<String, AnvizentDataType> structure)
	        throws UnsupportedException {
		LinkedHashMap<String, AnvizentDataType> newStructure = new LinkedHashMap<>();

		newStructure.put(SparkConstants.Kafka.TOPIC, new AnvizentDataType(String.class));
		newStructure.put(SparkConstants.Kafka.KEY, new AnvizentDataType(String.class));
		newStructure.put(SparkConstants.Kafka.VALUE, new AnvizentDataType(String.class));

		return newStructure;
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new KafkaSinkDocHelper(this);
	}

	@Override
	public String getName() {
		return Components.KAFKA_SINK.get(ConfigConstants.General.NAME);
	}

	@Override
	public Validator getValidator() {
		return new KafkaSinkValidator(this);
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
		if (internalRDDName.equals(getName())) {
			return new AnvizentStatsCalculator<LinkedHashMap<String, Object>, LinkedHashMap<String, Object>>(statsCategory, statsName);
		} else {
			return new AnvizentToRowStatsCalculator(statsCategory, statsName);
		}
	}

	@Override
	public void createAccumulators(ConfigAndMappingConfigBeans configAndMappingConfigBeans, StatsType globalStatsType) {
		StatsType statsType = getStatsType(configAndMappingConfigBeans.getStatsStore(), globalStatsType);

		if (statsType.equals(StatsType.NONE)) {
			return;
		} else {

			boolean componentLevel = isComponentLevel(configAndMappingConfigBeans.getMappingConfigBeans());
			createFactoryAccumulators(configAndMappingConfigBeans.getConfigBean(), statsType, componentLevel);
			createSpecialAccumulators(configAndMappingConfigBeans.getConfigBean(), statsType, componentLevel);
		}
	}

	@Override
	protected void createFactoryAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		if ((statsType.equals(StatsType.COMPONENT_LEVEL) && !componentLevel) || statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator inAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.IN, StatsNames.IN, !componentLevel, getStatsCalculator(StatsCategory.IN, StatsNames.IN, getName()));
			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), inAnvizentAccumulator);
		}

		if (statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator writtenAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.OUT, !componentLevel, getStatsCalculator(StatsCategory.OUT, StatsNames.OUT, getName()));
			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), writtenAnvizentAccumulator);
		}

		AnvizentAccumulator errorAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(), getName(),
		        StatsCategory.ERROR, StatsNames.ERROR, true, getStatsCalculator(StatsCategory.ERROR, StatsNames.ERROR, getName()));
		ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), errorAnvizentAccumulator);
	}

	@Override
	public void createSpecialAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		if (statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator inAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName() + " " + RDDSpecialNames.TO_ROW, StatsCategory.IN, StatsNames.IN, !componentLevel,
			        getStatsCalculator(StatsCategory.IN, StatsNames.IN, getName() + " " + RDDSpecialNames.TO_ROW));
			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName() + " " + RDDSpecialNames.TO_ROW, inAnvizentAccumulator);
		}

		AnvizentAccumulator writtenAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        getName() + " " + RDDSpecialNames.TO_ROW, StatsCategory.OUT, StatsNames.WRITTEN, true,
		        getStatsCalculator(StatsCategory.OUT, StatsNames.WRITTEN, getName() + " " + RDDSpecialNames.TO_ROW));
		AnvizentAccumulator errorAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        getName() + " " + RDDSpecialNames.TO_ROW, StatsCategory.ERROR, StatsNames.ERROR, true,
		        getStatsCalculator(StatsCategory.ERROR, StatsNames.ERROR, getName() + " " + RDDSpecialNames.TO_ROW));

		ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName() + " " + RDDSpecialNames.TO_ROW, writtenAnvizentAccumulator,
		        errorAnvizentAccumulator);
	}

	@Override
	public ResourceValidator getResourceConfigValidator() {
		// TODO Auto-generated method stub
		return null;
	}
}

package com.anvizent.elt.core.spark.operation.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentRetrievalStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.MLM;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Components;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.RetryMandatoryFactory;
import com.anvizent.elt.core.spark.listener.responder.MLMTaskKillResponder;
import com.anvizent.elt.core.spark.mapping.service.DuplicateService;
import com.anvizent.elt.core.spark.operation.bean.KafkaRequestMetricsSpec;
import com.anvizent.elt.core.spark.operation.bean.KafkaRequestSpec;
import com.anvizent.elt.core.spark.operation.bean.KafkaRequestSpecConsumerProperties;
import com.anvizent.elt.core.spark.operation.bean.KafkaRequestSpecIOConfig;
import com.anvizent.elt.core.spark.operation.config.bean.MLMLookupConfigBean;
import com.anvizent.elt.core.spark.operation.doc.helper.MLMLookupDocHelper;
import com.anvizent.elt.core.spark.operation.function.MLMLookupFunction;
import com.anvizent.elt.core.spark.operation.service.MLMLookupService;
import com.anvizent.elt.core.spark.operation.validator.MLMLookupValidator;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;
import com.anvizent.rest.util.RestUtil;

import flexjson.JSONSerializer;
import scala.Tuple2;

/**
 * @author Hareen Bejjanki
 *
 */
public class MLMLookupFactory extends SimpleOperationFactory implements RetryMandatoryFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected Component process(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		MLMLookupConfigBean mlmLookupConfigBean = (MLMLookupConfigBean) configBean;

		validate(mlmLookupConfigBean, component.getStructure());

		KafkaRequestSpec kafkaRequestSpec = getEmptyKafkaRequestSpec();
		LinkedHashMap<String, AnvizentDataType> newStructure = getNewStructure(component.getStructure(), kafkaRequestSpec, mlmLookupConfigBean);
		String druidDataSourceInfoId = initiateSupervisor(mlmLookupConfigBean, kafkaRequestSpec);

		component.addELTCoreJobListenerIfNotExists("mlmlookup", new MLMTaskKillResponder(mlmLookupConfigBean, kafkaRequestSpec, druidDataSourceInfoId));

		JavaPairRDD<HashMap<String, Object>, Long> indexedPairRDD = component.getRDD(configBean.getSourceStream()).zipWithIndex();
		if (mlmLookupConfigBean.isPersist()) {
			indexedPairRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
		}

		JavaRDD<HashMap<String, Object>> javaRDD = indexedPairRDD.flatMap(new MLMLookupFunction(configBean, null, component.getStructure(), newStructure,
		        ApplicationBean.getInstance().getAccumulators(configBean.getName(), getName()),
		        ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink, getName()),
		        ErrorHandlerUtil.getJobDetails(configBean, getName())));

		return Component.createComponent(component.getSparkSession(), configBean.getName(), javaRDD, newStructure);
	}

	private void validate(MLMLookupConfigBean mlmLookupConfigBean, LinkedHashMap<String, AnvizentDataType> linkedHashMap) {
		if (mlmLookupConfigBean.isIncremental() && !isDimensionsAsKeyFields(mlmLookupConfigBean)) {
			String missingFields = "";
			for (String keyField : mlmLookupConfigBean.getKeyFields()) {
				if (!linkedHashMap.containsKey(keyField)) {
					if (!missingFields.isEmpty()) {
						missingFields = ", ";
					}

					missingFields += keyField;
				}
			}

			if (!missingFields.isEmpty()) {
				InvalidConfigException exception = new InvalidConfigException();
				exception.setDetails(mlmLookupConfigBean);
				exception.add(ExceptionMessage.FIELDS_ARE_NOT_PRESENT_IN_STRUCTURE, missingFields);
			}
		}
	}

	private KafkaRequestSpec getEmptyKafkaRequestSpec() {
		KafkaRequestSpecIOConfig kafkaRequestSpecIOConfig = new KafkaRequestSpecIOConfig();
		kafkaRequestSpecIOConfig.setConsumerProperties(new KafkaRequestSpecConsumerProperties());

		KafkaRequestSpec kafkaRequestSpec = new KafkaRequestSpec();
		kafkaRequestSpec.setDimensions(new ArrayList<>());
		kafkaRequestSpec.setIoConfig(kafkaRequestSpecIOConfig);
		kafkaRequestSpec.setMetricsSpec(new ArrayList<>());

		return kafkaRequestSpec;
	}

	public LinkedHashMap<String, AnvizentDataType> getNewStructure(LinkedHashMap<String, AnvizentDataType> structure, KafkaRequestSpec kafkaRequestSpec,
	        MLMLookupConfigBean mlmLookupConfigBean) throws UnsupportedException {
		String newKey = DuplicateService.getKey("", MLM.MEASURES_AS_DIMENSIONS_POST_FIX_DEFAULT);
		LinkedHashMap<String, AnvizentDataType> newStructure = new LinkedHashMap<String, AnvizentDataType>();

		duplicateStructure(structure, newKey, newStructure, kafkaRequestSpec, mlmLookupConfigBean);

		newStructure.put(MLM.ANVIZ_COUNT, new AnvizentDataType(Long.class));
		kafkaRequestSpec.getMetricsSpec().add(new KafkaRequestMetricsSpec(MLM.ANVIZ_COUNT));
		newStructure.put(MLM.ANVIZ_UUID, new AnvizentDataType(Long.class));
		kafkaRequestSpec.getDimensions().add(MLM.ANVIZ_UUID);
		newStructure.put(MLM.ANVIZ_COPY_OF, new AnvizentDataType(Long.class));
		kafkaRequestSpec.getDimensions().add(MLM.ANVIZ_COPY_OF);
		newStructure.put(MLM.TIME_FIELD, new AnvizentDataType(Long.class));

		return newStructure;
	}

	@SuppressWarnings("rawtypes")
	private void duplicateStructure(LinkedHashMap<String, AnvizentDataType> structure, String newKey, LinkedHashMap<String, AnvizentDataType> newStructure,
	        KafkaRequestSpec kafkaRequestSpec, MLMLookupConfigBean mlmLookupConfigBean) throws UnsupportedException {
		boolean dimensionsAsKeyFields = isDimensionsAsKeyFields(mlmLookupConfigBean);

		if (dimensionsAsKeyFields) {
			mlmLookupConfigBean.setKeyFields(new ArrayList<>());
		}

		for (Entry<String, AnvizentDataType> map : structure.entrySet()) {
			Class javaType = map.getValue().getJavaType();
			if (!Constants.Type.DIMENSIONS.contains(javaType)) {
				addToNewStructure(newStructure, StringUtils.replace(newKey, com.anvizent.elt.core.spark.constant.Constants.General.PLACEHOLDER, map.getKey()),
				        map, kafkaRequestSpec);
			} else {
				newStructure.put(map.getKey(), map.getValue());
				kafkaRequestSpec.getDimensions().add(map.getKey());

				if (dimensionsAsKeyFields) {
					mlmLookupConfigBean.getKeyFields().add(map.getKey());
				}
			}
		}
	}

	private boolean isDimensionsAsKeyFields(MLMLookupConfigBean mlmLookupConfigBean) {
		return mlmLookupConfigBean.isIncremental() && (mlmLookupConfigBean.getKeyFields() == null || mlmLookupConfigBean.getKeyFields().size() == 0);
	}

	private static void addToNewStructure(LinkedHashMap<String, AnvizentDataType> newStructure, String newKey, Entry<String, AnvizentDataType> map,
	        KafkaRequestSpec kafkaRequestSpec) throws UnsupportedException {
		newStructure.put(map.getKey(), map.getValue());
		kafkaRequestSpec.getMetricsSpec().add(new KafkaRequestMetricsSpec(map.getKey()));
		newStructure.put(newKey, new AnvizentDataType(String.class));
		kafkaRequestSpec.getDimensions().add(newKey);
	}

	private String initiateSupervisor(MLMLookupConfigBean mlmLookupConfigBean, KafkaRequestSpec kafkaRequestSpec) throws Exception {
		setKafkaRequestSpec(mlmLookupConfigBean, kafkaRequestSpec);

		return sendRequest(mlmLookupConfigBean, kafkaRequestSpec);
	}

	private String sendRequest(MLMLookupConfigBean mlmLookupConfigBean, KafkaRequestSpec kafkaRequestSpec) throws Exception {
		RestUtil restUtil = new RestUtil(true);

		ResponseEntity<String> exchange = restUtil.exchange(mlmLookupConfigBean.getInitiateURL(),
		        new JSONSerializer().exclude("*.class").include("*.*").serialize(kafkaRequestSpec), HttpMethod.POST,
		        MLMLookupService.getHeadersMap(mlmLookupConfigBean), mlmLookupConfigBean.getTableName(), mlmLookupConfigBean.getUserId(),
		        mlmLookupConfigBean.getClientId(), "kafka", mlmLookupConfigBean.isIncremental());

		MLMLookupService.validateResponse(exchange, mlmLookupConfigBean.getInitiateURL(), "initiate");
		HashMap<String, Object> map = restUtil.getMap(exchange);
		return map.get("data").toString();
	}

	private void setKafkaRequestSpec(MLMLookupConfigBean mlmLookupConfigBean, KafkaRequestSpec kafkaRequestSpec) {
		kafkaRequestSpec.getIoConfig().setTopic(mlmLookupConfigBean.getKafkaTopic());
		kafkaRequestSpec.getIoConfig().setReplicas(mlmLookupConfigBean.getReplicas());
		kafkaRequestSpec.getIoConfig().setTaskCount(mlmLookupConfigBean.getTaskCount());
		kafkaRequestSpec.getIoConfig().setTaskDuration(mlmLookupConfigBean.getTaskDuration());
		kafkaRequestSpec.getIoConfig().getConsumerProperties().setBootstrapServers(mlmLookupConfigBean.getKafkaBootstrapServers());
	}

	@Override
	public String getName() {
		return Components.MLM_LOOKUP.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new MLMLookupDocHelper(this);
	}

	@Override
	public Validator getValidator() {
		return new MLMLookupValidator(this);
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
		return new AnvizentRetrievalStatsCalculator<Tuple2<LinkedHashMap<String, Object>, Long>, Iterator<LinkedHashMap<String, Object>>>(statsCategory,
		        statsName);
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
		AnvizentAccumulator lookedupRowsAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        getName(), StatsCategory.OTHER, StatsNames.LOOKEDUP_ROWS, true, getStatsCalculator(StatsCategory.OTHER, StatsNames.LOOKEDUP_ROWS, getName()));

		ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), lookedupRowsAnvizentAccumulator);
	}

	@Override
	public ResourceValidator getResourceConfigValidator() {
		// TODO Auto-generated method stub
		return null;
	}
}

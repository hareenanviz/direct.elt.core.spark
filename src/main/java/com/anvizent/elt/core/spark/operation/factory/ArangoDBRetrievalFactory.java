package com.anvizent.elt.core.spark.operation.factory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.spark.api.java.JavaRDD;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;
import com.anvizent.elt.core.spark.factory.RetryMandatoryFactory;
import com.anvizent.elt.core.spark.listener.responder.ELTCoreJobResponder;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBRetrievalConfigBean;
import com.anvizent.elt.core.spark.operation.service.ArangoDBRetrievalFunctionService;
import com.anvizent.elt.core.spark.operation.validator.ArangoDBRetrievalValidator;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.util.StructureUtil;
import com.anvizent.elt.core.spark.validator.Validator;
import com.anvizent.query.builder.exception.UnderConstructionException;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class ArangoDBRetrievalFactory extends SimpleOperationFactory implements RetryMandatoryFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected Component process(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean = (ArangoDBRetrievalConfigBean) configBean;

		if (arangoDBRetrievalConfigBean.getCacheType().equals(CacheType.MEMCACHE)
		        || arangoDBRetrievalConfigBean.getCacheType().equals(CacheType.ELASTIC_CACHE)) {
			throw new UnimplementedException("'" + arangoDBRetrievalConfigBean.getCacheType() + "' is not implemented.");
		}

		return getComponent(arangoDBRetrievalConfigBean, component, errorHandlerSink);
	}

	private Component getComponent(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean, Component component, ErrorHandlerSink errorHandlerSink)
	        throws Exception {
		LinkedHashMap<String, AnvizentDataType> newStructure = getNewLookUpStructure(arangoDBRetrievalConfigBean, component);

		ArangoDBRetrievalFunctionService.setInsertValues(arangoDBRetrievalConfigBean);

		buildQueries(arangoDBRetrievalConfigBean);

		component.addELTCoreJobListenerIfNotExists(CacheType.EHCACHE.name(), new ELTCoreJobResponder());

		JavaRDD<HashMap<String, Object>> retrievalRDD = getRetrievalRDD(arangoDBRetrievalConfigBean, component, newStructure, errorHandlerSink);

		return Component.createComponent(component.getSparkSession(), arangoDBRetrievalConfigBean.getName(), retrievalRDD, newStructure);
	}

	private void buildQueries(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean)
	        throws InvalidSituationException, UnsupportedCoerceException, UnderConstructionException, DateParseException {
		ArangoDBRetrievalFunctionService.buildInsertQuery(arangoDBRetrievalConfigBean);
	}

	private LinkedHashMap<String, AnvizentDataType> getNewLookUpStructure(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean, Component component)
	        throws ImproperValidationException, ClassNotFoundException, InvalidConfigException, SQLException, UnsupportedException, UnimplementedException,
	        TimeoutException {
		return StructureUtil.getNewStructure(arangoDBRetrievalConfigBean, component.getStructure(),
		        arangoDBRetrievalConfigBean.getSelectFieldAliases() == null || arangoDBRetrievalConfigBean.getSelectFieldAliases().isEmpty()
		                ? arangoDBRetrievalConfigBean.getSelectFields()
		                : arangoDBRetrievalConfigBean.getSelectFieldAliases(),
		        arangoDBRetrievalConfigBean.getSelectFieldTypes(), arangoDBRetrievalConfigBean.getSelectFieldPositions(),
		        Operation.General.SELECT_COLUMNS_AS_FIELDS, Operation.General.SELECT_FIELD_POSITIONS);
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
	public Validator getValidator() {
		return getArangoDBRetrievalValidator();
	}

	@Override
	public SchemaValidator getSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
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
		ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean = (ArangoDBRetrievalConfigBean) configBean;

		AnvizentAccumulator lookupAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        getName(), StatsCategory.OTHER, StatsNames.LOOKUP_COUNT, true, getStatsCalculator(StatsCategory.OTHER, StatsNames.LOOKUP_COUNT, getName()));
		AnvizentAccumulator lookedupRowsAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        getName(), StatsCategory.OTHER, StatsNames.LOOKEDUP_ROWS, true, getStatsCalculator(StatsCategory.OTHER, StatsNames.LOOKEDUP_ROWS, getName()));

		ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), lookupAnvizentAccumulator, lookedupRowsAnvizentAccumulator);

		if (arangoDBRetrievalConfigBean.getCacheType() != null && !arangoDBRetrievalConfigBean.getCacheType().equals(CacheType.NONE)) {
			AnvizentAccumulator cachedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OTHER, StatsNames.CACHE, true, getStatsCalculator(StatsCategory.OTHER, StatsNames.CACHE, getName()));
			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), cachedAnvizentAccumulator);
		}

		if (arangoDBRetrievalConfigBean.getOnZeroFetch().equals(OnZeroFetchOperation.INSERT)) {
			AnvizentAccumulator insertedAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.INSERTED, true, getStatsCalculator(StatsCategory.OUT, StatsNames.INSERTED, getName()));
			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), insertedAnvizentAccumulator);
		}
	}

	protected abstract JavaRDD<HashMap<String, Object>> getRetrievalRDD(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean, Component component,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ErrorHandlerSink errorHandlerSink) throws Exception;

	protected abstract ArangoDBRetrievalValidator getArangoDBRetrievalValidator();
}

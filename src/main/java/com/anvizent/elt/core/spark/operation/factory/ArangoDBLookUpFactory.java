package com.anvizent.elt.core.spark.operation.factory;

import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.spark.api.java.JavaRDD;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentRetrievalStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Components;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBRetrievalConfigBean;
import com.anvizent.elt.core.spark.operation.doc.helper.ArangoDBLookUpDocHelper;
import com.anvizent.elt.core.spark.operation.function.ArangoDBLookUpFunction;
import com.anvizent.elt.core.spark.operation.validator.ArangoDBLookUpValidator;
import com.anvizent.elt.core.spark.operation.validator.ArangoDBRetrievalValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ArangoDBLookUpFactory extends ArangoDBRetrievalFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected JavaRDD<HashMap<String, Object>> getRetrievalRDD(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean, Component component,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ErrorHandlerSink errorHandlerSink) throws Exception {
		JavaRDD<HashMap<String, Object>> lookUpRDD = component.getRDD(arangoDBRetrievalConfigBean.getSourceStream())
		        .flatMap(new ArangoDBLookUpFunction(arangoDBRetrievalConfigBean, component.getStructure(), newStructure,
		                ApplicationBean.getInstance().getAccumulators(arangoDBRetrievalConfigBean.getName(), getName()),
		                ErrorHandlerUtil.getErrorHandlerFunction(arangoDBRetrievalConfigBean, component.getStructure(), errorHandlerSink, getName()),
		                ErrorHandlerUtil.getJobDetails(arangoDBRetrievalConfigBean, getName())));

		return lookUpRDD;
	}

	@Override
	public String getName() {
		return Components.ARANGO_DB_LOOKUP.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new ArangoDBLookUpDocHelper(this);
	}

	@Override
	protected ArangoDBRetrievalValidator getArangoDBRetrievalValidator() {
		return new ArangoDBLookUpValidator(this);
	}

	@Override
	protected IStatsCalculator getStatsCalculator(StatsCategory statsCategory, String statsName, String internalRDDName) {
		return new AnvizentRetrievalStatsCalculator<LinkedHashMap<String, Object>, LinkedHashMap<String, Object>>(statsCategory, statsName);
	}

	@Override
	public ResourceValidator getResourceConfigValidator() {
		// TODO Auto-generated method stub
		return null;
	}

}

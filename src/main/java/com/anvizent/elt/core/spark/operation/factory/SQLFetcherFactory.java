package com.anvizent.elt.core.spark.operation.factory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.spark.api.java.JavaRDD;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
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
import com.anvizent.elt.core.spark.operation.config.bean.SQLRetrievalConfigBean;
import com.anvizent.elt.core.spark.operation.doc.helper.SQLFetcherDocHelper;
import com.anvizent.elt.core.spark.operation.function.SQLFetcherFunction;
import com.anvizent.elt.core.spark.operation.validator.SQLFetcherValidator;
import com.anvizent.elt.core.spark.operation.validator.SQLRetrievalValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLFetcherFactory extends SQLRetrievalFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected JavaRDD<HashMap<String, Object>> getRetrievalRDD(SQLRetrievalConfigBean sqlRetrievalConfigBean, Component component,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ErrorHandlerSink errorHandlerSink) throws InvalidRelationException, Exception {
		JavaRDD<HashMap<String, Object>> fetcherRDD = component.getRDD(sqlRetrievalConfigBean.getSourceStream())
		        .flatMap(new SQLFetcherFunction(sqlRetrievalConfigBean, component.getStructure(), newStructure,
		                ApplicationBean.getInstance().getAccumulators(sqlRetrievalConfigBean.getName(), getName()),
		                ErrorHandlerUtil.getErrorHandlerFunction(sqlRetrievalConfigBean, component.getStructure(), errorHandlerSink, getName()),
		                ErrorHandlerUtil.getJobDetails(sqlRetrievalConfigBean, getName())));

		return fetcherRDD;
	}

	@Override
	public String getName() {
		return Components.SQL_FETCHER.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new SQLFetcherDocHelper(this);
	}

	@Override
	protected SQLRetrievalValidator getSQLRetrievalValidator() {
		return new SQLFetcherValidator(this);
	}

	@Override
	protected IStatsCalculator getStatsCalculator(StatsCategory statsCategory, String statsName, String internalRDDName) {
		return new AnvizentRetrievalStatsCalculator<LinkedHashMap<String, Object>, Iterator<LinkedHashMap<String, Object>>>(statsCategory, statsName);
	}

	@Override
	public ResourceValidator getResourceConfigValidator() {
		// TODO Auto-generated method stub
		return null;
	}
}

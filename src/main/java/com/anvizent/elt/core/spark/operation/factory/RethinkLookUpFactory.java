package com.anvizent.elt.core.spark.operation.factory;

import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.spark.api.java.JavaRDD;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
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
import com.anvizent.elt.core.spark.operation.config.bean.RethinkRetrievalConfigBean;
import com.anvizent.elt.core.spark.operation.doc.helper.RethinkLookUpDocHelper;
import com.anvizent.elt.core.spark.operation.function.RethinkLookUpFunction;
import com.anvizent.elt.core.spark.operation.validator.RethinkLookUpValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class RethinkLookUpFactory extends RethinkRetrievalFactory {

	private static final long serialVersionUID = 1L;

	@Override
	public String getName() {
		return Components.RETHINK_LOOKUP.get(General.NAME);
	}

	@Override
	protected Validator getRethinkRetrievalValidator() {
		return new RethinkLookUpValidator(this);
	}

	@Override
	protected DocHelper getRethinkRetrievalDocHelper() throws InvalidParameter {
		return new RethinkLookUpDocHelper(this);
	}

	@Override
	protected JavaRDD<HashMap<String, Object>> getRetrievalRDD(RethinkRetrievalConfigBean rethinkRetrievalConfigBean, Component component,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		JavaRDD<HashMap<String, Object>> lookUpRDD = component.getRDD(rethinkRetrievalConfigBean.getSourceStream())
		        .flatMap(new RethinkLookUpFunction(rethinkRetrievalConfigBean, component.getStructure(), newStructure,
		                ApplicationBean.getInstance().getAccumulators(rethinkRetrievalConfigBean.getName(), getName()),
		                ErrorHandlerUtil.getErrorHandlerFunction(rethinkRetrievalConfigBean, component.getStructure(), errorHandlerSink, getName()),
		                ErrorHandlerUtil.getJobDetails(rethinkRetrievalConfigBean, getName())));

		return lookUpRDD;
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

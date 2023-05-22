package com.anvizent.elt.core.spark.sink.factory;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink.Components;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.sink.doc.helper.ConsoleSinkDocHelper;
import com.anvizent.elt.core.spark.sink.function.ConsoleSinkFunction;
import com.anvizent.elt.core.spark.sink.validator.ConsoleSinkValidator;
import com.anvizent.elt.core.spark.source.config.bean.ConsoleSinkConfigBean;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class ConsoleSinkFactory extends SinkFactory {
	private static final long serialVersionUID = 1L;

	@Override
	public void write(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		System.out.println(General.STRUCTURE + General.COLON + " " + component.getStructure());

		LinkedHashMap<String, AnvizentDataType> newStructure = getNewStructure(component.getStructure());

		AnvizentVoidFunction errorHandlerSinkFunction = ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink,
		        getName());
		JobDetails jobDetails = ErrorHandlerUtil.getJobDetails(configBean, getName());
		ArrayList<AnvizentAccumulator> anvizentAccumulators = ApplicationBean.getInstance().getAccumulators(configBean.getName(), getName());

		if (((ConsoleSinkConfigBean) configBean).isWriteAll()) {
			component.getRDD(configBean.getSourceStream()).foreach(new ConsoleSinkFunction(configBean, null, component.getStructure(), newStructure,
			        anvizentAccumulators, errorHandlerSinkFunction, jobDetails));
		} else {
			component.getRDDAsDataset(configBean.getSourceStream(), configBean, anvizentAccumulators, errorHandlerSinkFunction, jobDetails).show();
		}
	}

	private LinkedHashMap<String, AnvizentDataType> getNewStructure(LinkedHashMap<String, AnvizentDataType> structure) {
		return new LinkedHashMap<String, AnvizentDataType>(structure);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new ConsoleSinkDocHelper(this);
	}

	@Override
	public String getName() {
		return Components.CONSOLE_SINK.get(ConfigConstants.General.NAME);
	}

	@Override
	public Validator getValidator() {
		return new ConsoleSinkValidator(this);
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
		return new AnvizentStatsCalculator<>(statsCategory, statsName);
	}

	@Override
	public void createSpecialAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		// TODO Auto-generated method stub
	}

	@Override
	public ResourceValidator getResourceConfigValidator() {
		// TODO Auto-generated method stub
		return null;
	}
}

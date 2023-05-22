package com.anvizent.elt.core.spark.sink.factory;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentToRowStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.FileFormat;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.sink.config.bean.FileSinkConfigBean;
import com.anvizent.elt.core.spark.sink.doc.helper.CSVFileSinkDocHelper;
import com.anvizent.elt.core.spark.sink.validator.CSVFileSinkValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class CSVFileSinkFactory extends SinkFactory {
	private static final long serialVersionUID = 1L;

	@Override
	public void write(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		FileSinkConfigBean fileSinkConfigBean = (FileSinkConfigBean) configBean;

		Dataset<Row> dataset = component.getRDDAsDataset(fileSinkConfigBean.getSourceStream(), fileSinkConfigBean,
		        ApplicationBean.getInstance().getAccumulators(configBean.getName(), getName()),
		        ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink, getName()),
		        ErrorHandlerUtil.getJobDetails(configBean, getName()));

		if (fileSinkConfigBean.isSingleFile()) {
			dataset = dataset.coalesce(1);
		}

		DataFrameWriter<Row> dataFrameWriter = dataset.write();
		if (fileSinkConfigBean.getSaveMode() != null) {
			dataFrameWriter = dataFrameWriter.mode(fileSinkConfigBean.getSaveMode());
		}

		dataFrameWriter.options(fileSinkConfigBean.getOptions()).format(FileFormat.CSV.getValue()).save(fileSinkConfigBean.getPath());
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new CSVFileSinkDocHelper(this);
	}

	@Override
	public String getName() {
		return ConfigConstants.Sink.Components.CSV_SINK.get(General.NAME);
	}

	@Override
	public Validator getValidator() {
		return new CSVFileSinkValidator(this);
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
		return new AnvizentToRowStatsCalculator(statsCategory, statsName);
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

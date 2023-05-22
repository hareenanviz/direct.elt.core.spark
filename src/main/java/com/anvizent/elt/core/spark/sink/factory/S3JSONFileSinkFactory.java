package com.anvizent.elt.core.spark.sink.factory;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentToRowStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.config.bean.S3Connection;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.constant.FileFormat;
import com.anvizent.elt.core.spark.constant.SparkConstants;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.sink.config.bean.S3FileSinkConfigBean;
import com.anvizent.elt.core.spark.sink.doc.helper.S3JSONFileSinkDocHelper;
import com.anvizent.elt.core.spark.sink.validator.S3JSONFileSinkValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class S3JSONFileSinkFactory extends SinkFactory {
	private static final long serialVersionUID = 1L;

	@Override
	protected void write(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		S3FileSinkConfigBean s3jsonFileSinkConfigBean = (S3FileSinkConfigBean) configBean;
		S3Connection s3Connection = s3jsonFileSinkConfigBean.getS3Connection();

		component.getSparkSession().sparkContext().hadoopConfiguration().set(SparkConstants.Config.S3_AWS_ACCESS_KEY_ID, s3Connection.getAccessKey());
		component.getSparkSession().sparkContext().hadoopConfiguration().set(SparkConstants.Config.S3_AWS_SECRET_ACCESS_KEY, s3Connection.getSecretKey());
		component.getSparkSession().sparkContext().hadoopConfiguration().set(SparkConstants.Config.S3_IMPL, SparkConstants.S3.NATIVE_S3_FILE_SYSTEM);

		AnvizentVoidFunction errorHandlerSinkFunction = ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink,
		        getName());
		JobDetails jobDetails = ErrorHandlerUtil.getJobDetails(configBean, getName());
		Dataset<Row> dataset = component.getRDDAsDataset(s3jsonFileSinkConfigBean.getSourceStream(), s3jsonFileSinkConfigBean,
		        ApplicationBean.getInstance().getAccumulators(configBean.getName(), getName()), errorHandlerSinkFunction, jobDetails);

		if (s3jsonFileSinkConfigBean.isSingleFile()) {
			dataset = dataset.coalesce(1);
		}

		DataFrameWriter<Row> dataFrameWriter = dataset.write();
		if (s3jsonFileSinkConfigBean.getSaveMode() != null) {
			dataFrameWriter = dataFrameWriter.mode(s3jsonFileSinkConfigBean.getSaveMode());
		}

		dataFrameWriter.format(FileFormat.JSON.getValue()).save(Constants.Protocol.S3 + s3Connection.getBucketName() + s3jsonFileSinkConfigBean.getPath());
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new S3JSONFileSinkDocHelper(this);
	}

	@Override
	public String getName() {
		return ConfigConstants.Sink.Components.S3_JSON_SINK.get(General.NAME);
	}

	@Override
	public Validator getValidator() {
		return new S3JSONFileSinkValidator(this);
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

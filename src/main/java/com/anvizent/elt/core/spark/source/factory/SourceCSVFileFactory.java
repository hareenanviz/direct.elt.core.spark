package com.anvizent.elt.core.spark.source.factory;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Source;
import com.anvizent.elt.core.spark.constant.FileFormat;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.source.config.bean.SourceCSVFileConfigBean;
import com.anvizent.elt.core.spark.source.doc.helper.SourceCSVFileDocHelper;
import com.anvizent.elt.core.spark.source.validator.SourceCSVFileValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class SourceCSVFileFactory extends SQLSourceFactory {

	private static final long serialVersionUID = 1L;

	@Override
	public Dataset<Row> read(ConfigBean configBean) throws ImproperValidationException, UnsupportedException {
		SourceCSVFileConfigBean sourceCSVFileConfigBean = (SourceCSVFileConfigBean) configBean;

		DataFrameReader dataFrameReader = ApplicationBean.getInstance().getSparkSession().read();

		if (sourceCSVFileConfigBean.getOptions() != null && !sourceCSVFileConfigBean.getOptions().isEmpty()) {
			dataFrameReader = dataFrameReader.options(sourceCSVFileConfigBean.getOptions());
		}

		if (sourceCSVFileConfigBean.getStructType() != null) {
			dataFrameReader = dataFrameReader.schema(sourceCSVFileConfigBean.getStructType());
		}

		return dataFrameReader.format(FileFormat.CSV.getValue()).load(sourceCSVFileConfigBean.getPath());
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new SourceCSVFileDocHelper(this);
	}

	@Override
	public String getName() {
		return Source.Components.SOURCE_CSV.get(General.NAME);
	}

	@Override
	public Validator getValidator() {
		return new SourceCSVFileValidator(this);
	}

	@Override
	public SchemaValidator getSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getMaxInputs() {
		return 0;
	}

	@Override
	public Integer getMinInputs() {
		return 0;
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
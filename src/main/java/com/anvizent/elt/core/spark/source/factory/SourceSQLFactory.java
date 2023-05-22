package com.anvizent.elt.core.spark.source.factory;

import java.util.HashMap;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.spark.common.util.EitherOr;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Source.Components;
import com.anvizent.elt.core.spark.constant.SparkConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.source.config.bean.SourceSQLConfigBean;
import com.anvizent.elt.core.spark.source.doc.helper.SourceSQLDocHelper;
import com.anvizent.elt.core.spark.source.pojo.StructTypeAndJavaRDD;
import com.anvizent.elt.core.spark.source.rdd.SourceSQLBatchPartitionedRDD;
import com.anvizent.elt.core.spark.source.rdd.service.SourceSQL_RDDService;
import com.anvizent.elt.core.spark.source.resource.validator.SourceSQLResourceValidator;
import com.anvizent.elt.core.spark.source.validator.SourceSQLValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class SourceSQLFactory extends ComplexSourceFactory {

	private static final long serialVersionUID = 1L;
	private final SourceSQL_RDDService service = new SourceSQL_RDDService();

	@SuppressWarnings({ "rawtypes" })
	@Override
	public EitherOr<Dataset<Row>, StructTypeAndJavaRDD<HashMap>> read(ConfigBean configBean)
	        throws UnsupportedException, UnimplementedException, RecordProcessingException {
		SourceSQLConfigBean sqlConfigBean = (SourceSQLConfigBean) configBean;
		EitherOr<Dataset<Row>, StructTypeAndJavaRDD<HashMap>> data = new EitherOr<>();

		if (sqlConfigBean.getNumberOfPartitions() != null || sqlConfigBean.getPartitionsSize() != null) {
			data.setB(getJavaRDD(sqlConfigBean));
		} else {
			data.setA(getDataset(sqlConfigBean));
		}

		return data;
	}

	@SuppressWarnings("rawtypes")
	private StructTypeAndJavaRDD<HashMap> getJavaRDD(SourceSQLConfigBean sqlConfigBean)
	        throws UnimplementedException, RecordProcessingException, UnsupportedException {
		JavaRDD<HashMap> rdd = new SourceSQLBatchPartitionedRDD(ApplicationBean.getInstance().getSparkSession().sparkContext(), sqlConfigBean).toJavaRDD();
		return new StructTypeAndJavaRDD<>(rdd, service.getStructType(sqlConfigBean));
	}

	private Dataset<Row> getDataset(SourceSQLConfigBean sqlConfigBean) {
		RDBMSConnection rdbmsConnection = sqlConfigBean.getConnection();

		Dataset<Row> dataSet = null;

		if (sqlConfigBean.getOptions() != null && !sqlConfigBean.getOptions().isEmpty()) {
			dataSet = ApplicationBean.getInstance().getSparkSession().read().options(sqlConfigBean.getOptions()).jdbc(rdbmsConnection.getJdbcURL(),
			        sqlConfigBean.getTableName(), getProperties(rdbmsConnection));
		} else {
			dataSet = ApplicationBean.getInstance().getSparkSession().read().jdbc(rdbmsConnection.getJdbcURL(), sqlConfigBean.getTableName(),
			        getProperties(rdbmsConnection));
		}

		return dataSet;
	}

	private static Properties getProperties(RDBMSConnection rdbmsConnection) {
		Properties properties = new Properties();

		properties.put(SQLNoSQL.USER, rdbmsConnection.getUserName());
		properties.put(SQLNoSQL.PASSWORD, rdbmsConnection.getPassword());
		properties.put(SQLNoSQL.DRIVER, rdbmsConnection.getDriver());

		return properties;
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new SourceSQLDocHelper(this);
	}

	@Override
	public String getName() {
		return Components.SOURCE_SQL.get(General.NAME);
	}

	@Override
	public Validator getValidator() {
		return new SourceSQLValidator(this);
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
		return new SourceSQLResourceValidator();
	}
}

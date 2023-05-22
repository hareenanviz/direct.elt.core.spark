package com.anvizent.elt.core.spark.source.factory;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Source.Components;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.RetryMandatoryFactory;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.source.doc.helper.SourceArangoDBDocHelper;
import com.anvizent.elt.core.spark.source.rdd.SourceArangoDBRDD;
import com.anvizent.elt.core.spark.source.validator.SourceArangoDBValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class SourceArangoDBFactory extends CoreSourceFactory implements RetryMandatoryFactory {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings({ "resource", "rawtypes", "unchecked" })
	@Override
	protected JavaRDD read(ConfigBean configBean) throws Exception {
		JavaSparkContext javaSparkContext = new JavaSparkContext(ApplicationBean.getInstance().getSparkSession().sparkContext());

		JavaRDD javaRDD = new SourceArangoDBRDD(javaSparkContext.sc(), configBean).toJavaRDD();

		return javaRDD;
	}

	@Override
	public String getName() {
		return Components.SOURCE_ARANGODB.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new SourceArangoDBDocHelper(this);
	}

	@Override
	public Validator getValidator() {
		return new SourceArangoDBValidator(this);
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

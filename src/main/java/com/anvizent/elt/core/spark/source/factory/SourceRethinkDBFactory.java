package com.anvizent.elt.core.spark.source.factory;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

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
import com.anvizent.elt.core.spark.source.config.bean.SourceRethinkDBConfigBean;
import com.anvizent.elt.core.spark.source.doc.helper.SourceRethinkDBDocHelper;
import com.anvizent.elt.core.spark.source.rdd.SourceRethinkDBRDD;
import com.anvizent.elt.core.spark.source.validator.SourceRethinkDBValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class SourceRethinkDBFactory extends CoreSourceFactory implements RetryMandatoryFactory {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings({ "resource", "rawtypes", "unchecked" })
	@Override
	protected JavaRDD read(ConfigBean configBean) throws Exception {
		SourceRethinkDBConfigBean rethinkDBConfigBean = (SourceRethinkDBConfigBean) configBean;

		JavaSparkContext javaSparkContext = new JavaSparkContext(ApplicationBean.getInstance().getSparkSession().sparkContext());

		setTimeZoneDetails(rethinkDBConfigBean);
		JavaRDD javaRDD = new SourceRethinkDBRDD(javaSparkContext.sc(), configBean).toJavaRDD();

		return javaRDD;
	}

	private void setTimeZoneDetails(SourceRethinkDBConfigBean rethinkDBConfigBean) {
		ZoneOffset timeZoneOffset = OffsetDateTime.now().getOffset();
		rethinkDBConfigBean.setTimeZoneOffset(timeZoneOffset);
	}

	@Override
	public String getName() {
		return Components.SOURCE_RETHINKDB.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new SourceRethinkDBDocHelper(this);
	}

	@Override
	public Validator getValidator() {
		return new SourceRethinkDBValidator(this);
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

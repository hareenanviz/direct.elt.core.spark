package com.anvizent.elt.core.spark.source.rdd;

import java.util.LinkedHashMap;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.spark.source.config.bean.SourceRethinkDBConfigBean;
import com.anvizent.elt.core.spark.source.rdd.service.SourceRethinkDBRDDService;
import com.anvizent.universal.query.json.RangeDetails;
import com.anvizent.universal.query.json.UniversalQueryJson;

import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings({ "unused", "rawtypes" })
public class SourceRethinkDBRDD extends RDD<LinkedHashMap> {

	private static final long serialVersionUID = 1L;

	private static final ClassTag<LinkedHashMap> ROW_TAG = ClassManifestFactory$.MODULE$.fromClass(LinkedHashMap.class);

	private SourceRethinkDBConfigBean rethinkDBConfigBean;
	private UniversalQueryJson universalQueryJson;

	public SourceRethinkDBRDD(SparkContext sparkContext, ConfigBean configBean) {
		super(sparkContext, new ArrayBuffer<Dependency<?>>(), ROW_TAG);
		this.rethinkDBConfigBean = (SourceRethinkDBConfigBean) configBean;
		this.universalQueryJson = SourceRethinkDBRDDService.buildUniversalQueryJson(rethinkDBConfigBean);
	}

	@Override
	public Iterator<LinkedHashMap> compute(Partition arg0, TaskContext arg1) {
		RecordProcessingException recordProcessingException = null;
		int i = 0;

		do {
			recordProcessingException = null;
			try {
				return SourceRethinkDBRDDService.compute(arg0, rethinkDBConfigBean, universalQueryJson);
			} catch (RecordProcessingException processingException) {
				recordProcessingException = processingException;

				if (rethinkDBConfigBean.getRetryDelay() != null) {
					try {
						Thread.sleep(rethinkDBConfigBean.getRetryDelay());
					} catch (InterruptedException e) {
					}
				}
			} // TODO validation violation exception

		} while (++i < rethinkDBConfigBean.getMaxRetryCount());

		if (recordProcessingException != null) {
			throw new RuntimeException(recordProcessingException.getMessage(), recordProcessingException);
		}

		return null;
	}

	@Override
	public Partition[] getPartitions() {
		// TODO another retry bean for partitions
		RecordProcessingException recordProcessingException = null;
		int i = 0;

		do {
			recordProcessingException = null;
			try {
				return SourceRethinkDBRDDService.generatePartitions(rethinkDBConfigBean, universalQueryJson);
			} catch (RecordProcessingException processingException) {
				recordProcessingException = processingException;

				if (rethinkDBConfigBean.getRetryDelay() != null) {
					try {
						Thread.sleep(rethinkDBConfigBean.getRetryDelay());
					} catch (InterruptedException e) {
					}
				}
			} // TODO validation violation exception
		} while (++i < rethinkDBConfigBean.getMaxRetryCount());

		if (recordProcessingException != null) {
			throw new RuntimeException(recordProcessingException.getMessage(), recordProcessingException);
		}

		return null;
	}
}

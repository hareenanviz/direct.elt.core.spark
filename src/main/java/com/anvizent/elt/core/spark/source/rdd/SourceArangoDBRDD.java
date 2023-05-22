package com.anvizent.elt.core.spark.source.rdd;

import java.util.LinkedHashMap;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.spark.source.config.bean.SourceArangoDBConfigBean;
import com.anvizent.elt.core.spark.source.rdd.service.SourceArangoDBRDDService;

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
public class SourceArangoDBRDD extends RDD<LinkedHashMap> {

	private static final long serialVersionUID = 1L;

	private static final ClassTag<LinkedHashMap> ROW_TAG = ClassManifestFactory$.MODULE$.fromClass(LinkedHashMap.class);

	private SourceArangoDBConfigBean sourceArangoDBConfigBean;

	public SourceArangoDBRDD(SparkContext sparkContext, ConfigBean configBean) {
		super(sparkContext, new ArrayBuffer<Dependency<?>>(), ROW_TAG);
		this.sourceArangoDBConfigBean = (SourceArangoDBConfigBean) configBean;
	}

	@Override
	public Iterator<LinkedHashMap> compute(Partition arg0, TaskContext arg1) {
		RecordProcessingException recordProcessingException = null;
		int i = 0;

		do {
			recordProcessingException = null;
			try {
				return SourceArangoDBRDDService.compute(arg0, sourceArangoDBConfigBean);
			} catch (RecordProcessingException processingException) {
				recordProcessingException = processingException;

				if (sourceArangoDBConfigBean.getRetryDelay() != null) {
					try {
						Thread.sleep(sourceArangoDBConfigBean.getRetryDelay());
					} catch (InterruptedException e) {
					}
				}
			} // TODO validation violation exception

		} while (++i < sourceArangoDBConfigBean.getMaxRetryCount());

		if (recordProcessingException != null) {
			throw new RuntimeException(recordProcessingException.getMessage(), recordProcessingException);
		}

		return null;
	}

	@Override
	public Partition[] getPartitions() {
		RecordProcessingException recordProcessingException = null;
		int i = 0;

		do {
			recordProcessingException = null;
			try {
				return SourceArangoDBRDDService.generatePartitions(sourceArangoDBConfigBean);
			} catch (RecordProcessingException processingException) {
				recordProcessingException = processingException;

				if (sourceArangoDBConfigBean.getRetryDelay() != null) {
					try {
						Thread.sleep(sourceArangoDBConfigBean.getRetryDelay());
					} catch (InterruptedException e) {
					}
				}
			} // TODO validation violation exception
		} while (++i < sourceArangoDBConfigBean.getMaxRetryCount());

		if (recordProcessingException != null) {
			throw new RuntimeException(recordProcessingException.getMessage(), recordProcessingException);
		}

		return null;
	}
}

package com.anvizent.elt.core.spark.source.rdd;

import java.util.HashMap;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.spark.source.config.bean.SourceSQLConfigBean;
import com.anvizent.elt.core.spark.source.pojo.OffsetPartition;
import com.anvizent.elt.core.spark.source.rdd.service.ListOfHashMapIterator;
import com.anvizent.elt.core.spark.source.rdd.service.SourceSQL_RDDService;

import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

/**
 * @author Hareen Bejjanki
 *
 */
@SuppressWarnings({ "unused", "rawtypes" })
public class SourceSQLBatchPartitionedRDD extends RDD<HashMap> {

	private static final long serialVersionUID = 1L;

	private static final ClassTag<HashMap> ROW_TAG = ClassManifestFactory$.MODULE$.fromClass(HashMap.class);

	private final SourceSQLConfigBean sourceSQLConfigBean;
	private final SourceSQL_RDDService service;

	public SourceSQLBatchPartitionedRDD(SparkContext sparkContext, ConfigBean configBean) {
		super(sparkContext, new ArrayBuffer<Dependency<?>>(), ROW_TAG);
		this.sourceSQLConfigBean = (SourceSQLConfigBean) configBean;
		this.service = new SourceSQL_RDDService();
	}

	@Override
	public Iterator<HashMap> compute(Partition partition, TaskContext taskContext) {
		try {
			return new ListOfHashMapIterator(service.getRows(sourceSQLConfigBean, (OffsetPartition) partition));
		} catch (RecordProcessingException recordProcessingException) {
			throw new RuntimeException(recordProcessingException.getMessage(), recordProcessingException);
		}
	}

	@Override
	public Partition[] getPartitions() {
		RecordProcessingException recordProcessingException = null;
		int i = 0;

		do {
			try {
				long tableSize = service.getTableCount(sourceSQLConfigBean);

				if (sourceSQLConfigBean.getNumberOfPartitions() != null) {
					return OffsetPartition.getPartitions(tableSize, sourceSQLConfigBean.getNumberOfPartitions());
				} else {
					return OffsetPartition.getPartitions(tableSize, sourceSQLConfigBean.getPartitionsSize());
				}
			} catch (RecordProcessingException processingException) {
				recordProcessingException = processingException;

				if (sourceSQLConfigBean.getRetryDelay() != null) {
					try {
						Thread.sleep(sourceSQLConfigBean.getRetryDelay());
					} catch (InterruptedException e) {
					}
				}
			}
		} while (++i < sourceSQLConfigBean.getMaxRetryCount());

		if (recordProcessingException != null) {
			throw new RuntimeException(recordProcessingException.getMessage(), recordProcessingException);
		}

		return new OffsetPartition[] { new OffsetPartition(0, 0, Long.MAX_VALUE) };
	}
}

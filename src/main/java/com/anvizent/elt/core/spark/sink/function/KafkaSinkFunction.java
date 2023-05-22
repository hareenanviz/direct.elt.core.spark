package com.anvizent.elt.core.spark.sink.function;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentFunction;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.spark.constant.KafkaSinkWriteFormat;
import com.anvizent.elt.core.spark.constant.SparkConstants;
import com.anvizent.elt.core.spark.sink.config.bean.KafkaSinkConfigBean;

import flexjson.JSONContext;
import flexjson.JSONSerializer;
import flexjson.transformer.DateTransformer;
import flexjson.transformer.Transformer;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class KafkaSinkFunction extends AnvizentFunction {

	private static final long serialVersionUID = 1L;

	public KafkaSinkFunction(KafkaSinkConfigBean kafkaSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
			LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
			JobDetails jobDetails) throws InvalidArgumentsException {
		super(kafkaSinkConfigBean, null, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public HashMap<String, Object> process(HashMap<String, Object> row) throws ValidationViolationException, DataCorruptedException {
		KafkaSinkConfigBean kafkaSinkConfigBean = (KafkaSinkConfigBean) configBean;
		HashMap<String, Object> newRow = new HashMap<>();

		String topic = getTopic(kafkaSinkConfigBean, row);
		String key = getKey(kafkaSinkConfigBean.getKeyField(), row);
		String value = getValue(kafkaSinkConfigBean.getFormat(), kafkaSinkConfigBean.getDateFormat(), row);

		newRow.put(SparkConstants.Kafka.TOPIC, topic);
		newRow.put(SparkConstants.Kafka.KEY, key);
		newRow.put(SparkConstants.Kafka.VALUE, value);

		return newRow;
	}

	private String getValue(KafkaSinkWriteFormat format, String dateFormat, HashMap<String, Object> row) {
		if (format == null || format.equals(KafkaSinkWriteFormat.JSON)) {
			if (dateFormat != null && !dateFormat.isEmpty()) {
				return new JSONSerializer().transform(new DateTransformer(dateFormat), Date.class).exclude("*.class").include("*.*").serialize(row);
			} else {
				return new JSONSerializer().transform(new Transformer() {
					@Override
					public void transform(Object value) {
						JSONContext.get().write(((Date) value).getTime() + "");
					}
				}, Date.class).exclude("*.class").include("*.*").serialize(row);
			}
		} else {
			return String.valueOf(row.get(row.keySet().iterator().next()));
		}
	}

	private String getKey(String keyField, HashMap<String, Object> row) {
		if (keyField != null) {
			return String.valueOf(row.remove(keyField));
		} else {
			return new Date().getTime() + "";
		}
	}

	private String getTopic(KafkaSinkConfigBean kafkaSinkConfigBean, HashMap<String, Object> row) {
		Object topicValue = null;

		if (kafkaSinkConfigBean.getTopicField() != null) {
			topicValue = row.remove(kafkaSinkConfigBean.getTopicField());
		}

		if (topicValue == null) {
			return kafkaSinkConfigBean.getTopic();
		} else {
			return String.valueOf(topicValue);
		}
	}
}

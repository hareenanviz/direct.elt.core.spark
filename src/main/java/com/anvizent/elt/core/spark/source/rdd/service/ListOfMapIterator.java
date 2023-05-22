package com.anvizent.elt.core.spark.source.rdd.service;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;

import scala.collection.AbstractIterator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings("rawtypes")
public class ListOfMapIterator extends AbstractIterator<LinkedHashMap> {

	private final ArrayList<Map<String, Object>> list;
	private StructType structType;
	private int index = 0;
	private ZoneOffset timeZoneOffset;

	public ListOfMapIterator(ArrayList<Map<String, Object>> list, StructType structType, ZoneOffset timeZoneOffset) {
		this.list = list;
		this.structType = structType;
		this.timeZoneOffset = timeZoneOffset;
	}

	@Override
	public boolean hasNext() {
		return index < list.size();
	}

	@Override
	public LinkedHashMap<String, Object> next() {
		LinkedHashMap<String, Object> row = new LinkedHashMap<>();
		StructField[] structFields = structType.fields();

		Object[] values = new Object[structFields.length];
		Map<String, Object> map = list.get(index++);

		for (int i = 0; i < structFields.length; i++) {
			try {
				Object fieldValue = map.get(structFields[i].name());
				if (fieldValue != null) {
					Class fieldType = fieldValue.getClass();
					values[i] = TypeConversionUtil.dataTypeConversion(fieldValue, fieldType, new AnvizentDataType(structFields[i].dataType()).getJavaType(),
					        null, null, null, timeZoneOffset);
				} else {
					values[i] = fieldValue;
				}
			} catch (UnsupportedCoerceException | InvalidSituationException | DateParseException | ImproperValidationException | InvalidConfigValueException
			        | UnsupportedException exception) {
				throw new RuntimeException(exception.getMessage(), exception);
			}

			row.put(structFields[i].name(), values[i]);
		}

		return row;
	}
}

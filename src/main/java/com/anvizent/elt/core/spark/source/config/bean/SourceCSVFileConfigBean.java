package com.anvizent.elt.core.spark.source.config.bean;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.Constants;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SourceCSVFileConfigBean extends ConfigBean implements SourceConfigBean, SQLSource {
	private static final long serialVersionUID = 1L;

	private String path;
	private Map<String, String> options = new LinkedHashMap<>();
	private StructType structType;

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public void addPrefix(String prefix) {
		if (prefix != null && !prefix.isEmpty()) {
			path = Constants.FILE_PATH_SEPARATOR + prefix + path;
		}
	}

	public Map<String, String> getOptions() {
		return options;
	}

	public void setOptions(Map<String, String> options) {
		this.options = options;
	}

	public void putOption(String key, String value) {
		this.options.put(key, value);
	}

	public StructType getStructType() {
		return structType;
	}

	public void setStructType(StructType structType) {
		this.structType = structType;
	}

	public void setStructure(ArrayList<String> keys, ArrayList<Class<?>> keyTypes, ArrayList<Integer> precisions, ArrayList<Integer> scales)
	        throws ImproperValidationException, UnsupportedException {
		if (keys != null && keyTypes != null) {
			StructField[] structFields = new StructField[keys.size()];

			for (int i = 0; i < keys.size(); i++) {
				Class<?> type = (keyTypes != null && keyTypes.size() > 0) ? keyTypes.get(i) : String.class;
				int precision = precisions == null ? General.DECIMAL_PRECISION : (precisions.get(i) == null ? General.DECIMAL_PRECISION : precisions.get(i));
				int scale = scales == null ? General.DECIMAL_SCALE : (scales.get(i) == null ? General.DECIMAL_SCALE : scales.get(i));
				structFields[i] = DataTypes.createStructField(keys.get(i), new AnvizentDataType(type, precision, scale).getSparkType(), true);
			}

			this.structType = DataTypes.createStructType(structFields);
		} else {
			this.structType = null;
		}
	}
}
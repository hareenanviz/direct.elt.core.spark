package com.anvizent.elt.core.spark.sink.config.bean;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.spark.sql.SaveMode;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class FileSinkConfigBean extends ConfigBean implements SinkConfigBean {

	private static final long serialVersionUID = 1L;

	private String path;
	private boolean singleFile;
	private SaveMode saveMode;
	private Map<String, String> options = new LinkedHashMap<String, String>();

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public boolean isSingleFile() {
		return singleFile;
	}

	public void setSingleFile(boolean singleFile) {
		this.singleFile = singleFile;
	}

	public SaveMode getSaveMode() {
		return saveMode;
	}

	public void setSaveMode(SaveMode saveMode) {
		this.saveMode = saveMode;
	}

	public Map<String, String> getOptions() {
		return options;
	}

	public void setOptions(Map<String, String> options) {
		this.options = options;
	}
}

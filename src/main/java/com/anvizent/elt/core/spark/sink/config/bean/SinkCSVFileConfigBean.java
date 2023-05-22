package com.anvizent.elt.core.spark.sink.config.bean;

import java.util.LinkedHashMap;
import java.util.Map;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.spark.constant.Constants;

/**
 * @author Hareen Bejjanki
 *
 */
public class SinkCSVFileConfigBean extends ConfigBean implements SinkConfigBean {
	private static final long serialVersionUID = 1L;

	private String path;
	private boolean singleFile;
	private boolean replace;
	private Map<String, String> options = new LinkedHashMap<>();

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

	public boolean isSingleFile() {
		return singleFile;
	}

	public void setSingleFile(boolean singleFile) {
		this.singleFile = singleFile;
	}

	public boolean isReplace() {
		return replace;
	}

	public void setReplace(boolean replace) {
		this.replace = replace;
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
}
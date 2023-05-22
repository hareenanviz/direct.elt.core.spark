package com.anvizent.elt.core.spark.mapping.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RenameConfigBean extends MappingConfigBean {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> renameFrom;
	private ArrayList<String> renameTo;

	public ArrayList<String> getRenameFrom() {
		return renameFrom;
	}

	public void setRenameFrom(ArrayList<String> renameFrom) {
		this.renameFrom = renameFrom;
	}

	public ArrayList<String> getRenameTo() {
		return renameTo;
	}

	public void setRenameTo(ArrayList<String> renameTo) {
		this.renameTo = renameTo;
	}

	public RenameConfigBean(ArrayList<String> renameFrom, ArrayList<String> renameTo) {
		this.renameFrom = renameFrom;
		this.renameTo = renameTo;
	}

}

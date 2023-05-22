package com.anvizent.elt.core.spark.operation.pojo;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.AnvizentDataType;

public class AnvizentTypesWithAIColumn {

	private ArrayList<AnvizentDataType> selectFieldDataTypes;
	private String autoIncrementColumn;

	public ArrayList<AnvizentDataType> getSelectFieldDataTypes() {
		return selectFieldDataTypes;
	}

	public void setSelectFieldDataTypes(ArrayList<AnvizentDataType> selectFieldDataTypes) {
		this.selectFieldDataTypes = selectFieldDataTypes;
	}

	public String getAutoIncrementColumn() {
		return autoIncrementColumn;
	}

	public void setAutoIncrementColumn(String autoIncrementColumn) {
		this.autoIncrementColumn = autoIncrementColumn;
	}

}

package elt.core.spark.mapping.function;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.function.AnvizentFunction;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.spark.mapping.config.bean.RepositionConfigBean;
import com.anvizent.elt.core.spark.mapping.function.RepositionMappingFunction;

import elt.core.spark.function.AnvizentFunctionMappingPositiveCallTest;

/**
 * @author Hareen Bejjanki
 *
 */
public class RepositionFewFieldsCallTest extends AnvizentFunctionMappingPositiveCallTest {

	private Date now = new Date();

	@Override
	public MappingConfigBean getConfigBean() {
		return new RepositionConfigBean(getFields(), getPositions());
	}

	private ArrayList<String> getFields() {
		ArrayList<String> fields = new ArrayList<>();
		fields.add("ID");
		fields.add("Company");
		fields.add("OrderDate");
		fields.add("Country");
		fields.add("PlantName");

		return fields;
	}

	private ArrayList<Integer> getPositions() {
		ArrayList<Integer> positions = new ArrayList<>();
		positions.add(-1);
		positions.add(0);
		positions.add(-2);
		positions.add(2);
		positions.add(1);

		return positions;
	}

	@Override
	public LinkedHashMap<String, Object> getSourceValues() {
		LinkedHashMap<String, Object> sourceValues = new LinkedHashMap<>();
		sourceValues.put("ID", 101);
		sourceValues.put("Company", "USA");
		sourceValues.put("WO", 6984451L);
		sourceValues.put("OrderDate", now);
		sourceValues.put("PONumber", 24143);
		sourceValues.put("Country", "UNITED STATES");
		sourceValues.put("CustomerName", "CONCAST METAL PRODUCTS CO");
		sourceValues.put("PlantName", "Tallmadge OH (1115)");
		sourceValues.put("OrderNumber", 23119);
		sourceValues.put("Product_Code", 4110201);

		return sourceValues;
	}

	@Override
	public HashMap<String, Object> getExpectedValues() {
		HashMap<String, Object> expectedValues = new HashMap<>();

		expectedValues.put("Company", "USA");
		expectedValues.put("PlantName", "Tallmadge OH (1115)");
		expectedValues.put("Country", "UNITED STATES");
		expectedValues.put("WO", 6984451L);
		expectedValues.put("PONumber", 24143);
		expectedValues.put("CustomerName", "CONCAST METAL PRODUCTS CO");
		expectedValues.put("OrderNumber", 23119);
		expectedValues.put("Product_Code", 4110201);
		expectedValues.put("OrderDate", now);
		expectedValues.put("ID", 101);

		return expectedValues;
	}

	@Override
	public LinkedHashMap<String, AnvizentDataType> getNewStructure() throws UnsupportedException {
		newStructure = new LinkedHashMap<>();
		newStructure.put("Company", structure.get("Company"));
		newStructure.put("PlantName", structure.get("PlantName"));
		newStructure.put("Country", structure.get("Country"));
		newStructure.put("WO", structure.get("WO"));
		newStructure.put("PONumber", structure.get("PONumber"));
		newStructure.put("CustomerName", structure.get("CustomerName"));
		newStructure.put("OrderNumber", structure.get("OrderNumber"));
		newStructure.put("Product_Code", structure.get("Product_Code"));
		newStructure.put("OrderDate", structure.get("OrderDate"));
		newStructure.put("ID", structure.get("ID"));

		return newStructure;
	}

	@Override
	public AnvizentFunction getFunction() throws UnsupportedException, InvalidArgumentsException, InvalidRelationException {
		return new RepositionMappingFunction(null, configBean, structure, newStructure, null, null,
		        getJobDetails(configBean.getComponentName(), configBean.getConfigName(), configBean.getMappingConfigName()));
	}

	public static JobDetails getJobDetails(String componentName, String configName, String internalRDDName) {
		JobDetails jobDetails = new JobDetails(ApplicationBean.getInstance().getSparkAppName(), componentName, configName, internalRDDName);

		return jobDetails;
	}

	@Override
	public void adjustNewStructure(LinkedHashMap<String, AnvizentDataType> newStructure) throws UnsupportedException {
	}
}

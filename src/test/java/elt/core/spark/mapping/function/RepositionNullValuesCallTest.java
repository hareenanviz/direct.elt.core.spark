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
public class RepositionNullValuesCallTest extends AnvizentFunctionMappingPositiveCallTest {

	private Date now = new Date();

	@Override
	public MappingConfigBean getConfigBean() {
		return new RepositionConfigBean(getFields(), getPositions());
	}

	private ArrayList<String> getFields() {
		ArrayList<String> fields = new ArrayList<>();
		fields.add("ID");
		fields.add("Company");
		fields.add("WO");
		fields.add("OrderDate");
		fields.add("PONumber");
		fields.add("Country");
		fields.add("CustomerName");
		fields.add("PlantName");
		fields.add("OrderNumber");
		fields.add("Product_Code");

		return fields;
	}

	private ArrayList<Integer> getPositions() {
		ArrayList<Integer> positions = new ArrayList<>();
		positions.add(4);
		positions.add(3);
		positions.add(2);
		positions.add(1);
		positions.add(0);
		positions.add(-5);
		positions.add(-4);
		positions.add(-3);
		positions.add(-2);
		positions.add(-1);

		return positions;
	}

	@Override
	public LinkedHashMap<String, Object> getSourceValues() {
		LinkedHashMap<String, Object> sourceValues = new LinkedHashMap<>();
		sourceValues.put("ID", 101);
		sourceValues.put("Company", "USA");
		sourceValues.put("WO", null);
		sourceValues.put("OrderDate", now);
		sourceValues.put("PONumber", 24143);
		sourceValues.put("Country", null);
		sourceValues.put("CustomerName", "CONCAST METAL PRODUCTS CO");
		sourceValues.put("PlantName", "Tallmadge OH (1115)");
		sourceValues.put("OrderNumber", null);
		sourceValues.put("Product_Code", null);

		return sourceValues;
	}

	@Override
	public HashMap<String, Object> getExpectedValues() {
		HashMap<String, Object> expectedValues = new HashMap<>();

		expectedValues.put("PONumber", 24143);
		expectedValues.put("OrderDate", now);
		expectedValues.put("WO", null);
		expectedValues.put("Company", "USA");
		expectedValues.put("ID", 101);
		expectedValues.put("Country", null);
		expectedValues.put("CustomerName", "CONCAST METAL PRODUCTS CO");
		expectedValues.put("PlantName", "Tallmadge OH (1115)");
		expectedValues.put("OrderNumber", null);
		expectedValues.put("Product_Code", null);

		return expectedValues;
	}

	@Override
	public LinkedHashMap<String, AnvizentDataType> getStructure() throws UnsupportedException {
		LinkedHashMap<String, AnvizentDataType> structure = new LinkedHashMap<>();

		structure.put("ID", new AnvizentDataType(Integer.class));
		structure.put("Company", new AnvizentDataType(String.class));
		structure.put("WO", new AnvizentDataType(Integer.class));
		structure.put("OrderDate", new AnvizentDataType(Date.class));
		structure.put("PONumber", new AnvizentDataType(Integer.class));
		structure.put("Country", new AnvizentDataType(String.class));
		structure.put("CustomerName", new AnvizentDataType(String.class));
		structure.put("PlantName", new AnvizentDataType(String.class));
		structure.put("OrderNumber", new AnvizentDataType(Long.class));
		structure.put("Product_Code", new AnvizentDataType(Long.class));

		return structure;
	}

	@Override
	public LinkedHashMap<String, AnvizentDataType> getNewStructure() throws UnsupportedException {
		newStructure = new LinkedHashMap<>();
		newStructure.put("PONumber", structure.get("PONumber"));
		newStructure.put("OrderDate", structure.get("OrderDate"));
		newStructure.put("WO", structure.get("WO"));
		newStructure.put("Company", structure.get("Company"));
		newStructure.put("ID", structure.get("ID"));
		newStructure.put("Country", structure.get("Country"));
		newStructure.put("CustomerName", structure.get("CustomerName"));
		newStructure.put("PlantName", structure.get("PlantName"));
		newStructure.put("OrderNumber", structure.get("OrderNumber"));
		newStructure.put("Product_Code", structure.get("Product_Code"));

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

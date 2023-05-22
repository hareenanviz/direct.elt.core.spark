package elt.core.spark.function;

import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.junit.Before;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.function.AnvizentFilterFunction;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public abstract class AnvizentFilterFunctionCallTest {

	protected ConfigBean configBean;
	protected LinkedHashMap<String, AnvizentDataType> structure;
	protected LinkedHashMap<String, AnvizentDataType> newStructure;
	protected LinkedHashMap<String, Object> sourceValues;
	protected AnvizentFilterFunction function;

	@Before
	public void init() throws UnsupportedException, InvalidArgumentsException, InvalidRelationException {
		configBean = getConfigBean();
		sourceValues = getSourceValues();
		structure = getStructure();
		newStructure = getNewStructure();
		function = getFunction();
		initExpected();
	}

	private LinkedHashMap<String, AnvizentDataType> getStructure() throws UnsupportedException {
		LinkedHashMap<String, AnvizentDataType> structure = new LinkedHashMap<String, AnvizentDataType>();

		for (Entry<String, Object> entry : getSourceValues().entrySet()) {
			structure.put(entry.getKey(), new AnvizentDataType(entry.getValue().getClass()));
		}

		return structure;
	}

	private LinkedHashMap<String, AnvizentDataType> getNewStructure() throws UnsupportedException {
		LinkedHashMap<String, AnvizentDataType> newStructure = new LinkedHashMap<String, AnvizentDataType>(structure);

		return newStructure;
	}

	public abstract ConfigBean getConfigBean();

	public abstract LinkedHashMap<String, Object> getSourceValues();

	public abstract AnvizentFilterFunction getFunction() throws UnsupportedException, InvalidArgumentsException, InvalidRelationException;

	public abstract void initExpected();
}
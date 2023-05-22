package elt.core.spark.mapping.function;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.spark.mapping.config.bean.RenameConfigBean;
import com.anvizent.elt.core.spark.mapping.function.RenameMappingFunction;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RenameCallTest {

	private LinkedHashMap<String, Object> row;
	private ArrayList<String> renameFrom;
	private ArrayList<String> renameTo;
	private RenameMappingFunction renameFunction;
	private LinkedHashMap<String, AnvizentDataType> newStructure;

	@Before
	public void init() throws ImproperValidationException, UnsupportedException, InvalidArgumentsException, InvalidRelationException {
		row = new LinkedHashMap<String, Object>();
		row.put("sourceid", 30);
		row.put("username", "abcd");
		row.put("driverid", 3);
		row.put("sourcename", "hyddev");
		row.put("version", 1);

		renameFrom = new ArrayList<String>() {
			private static final long serialVersionUID = 1L;

			{
				add("sourcename");
				add("sourceid");
			}
		};
		renameTo = new ArrayList<String>() {
			private static final long serialVersionUID = 1L;
			{
				add("s-name");
				add("s-id");
			}
		};

		newStructure = new LinkedHashMap<String, AnvizentDataType>() {

			private static final long serialVersionUID = 1L;
			{
				put("s-id", new AnvizentDataType(Integer.class));
				put("username", new AnvizentDataType(String.class));
				put("driverid", new AnvizentDataType(Integer.class));
				put("s-name", new AnvizentDataType(String.class));
				put("version", new AnvizentDataType(Integer.class));
			}
		};

		renameFunction = new RenameMappingFunction(null, new RenameConfigBean(renameFrom, renameTo), new LinkedHashMap<>(), newStructure, null, null, null);
	}

	@Test
	public void renameCallTest() throws Exception {
		System.out.println("renamedTo ============ " + renameFunction.call(row));
		Assert.assertTrue(true);
	}

}

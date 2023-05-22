package elt.core.spark.filter.function;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.function.AnvizentFilterFunction;
import com.anvizent.elt.core.spark.filter.config.bean.FilterByExpressionConfigBean;
import com.anvizent.elt.core.spark.filter.function.FilterByExpressionFunction;
import com.anvizent.elt.core.spark.operation.service.ExpressionService;

import elt.core.spark.function.AnvizentFilterFunctionPositiveCallTest;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FilterByExpressionCallTest extends AnvizentFilterFunctionPositiveCallTest {

	@Override
	public Boolean getExpectedValues() {
		return Boolean.TRUE;
	}

	@Override
	public ConfigBean getConfigBean() {
		FilterByExpressionConfigBean configBean = new FilterByExpressionConfigBean();

		configBean.setExpressions(getExpressions());
		configBean.setArgumentFields(getArgumentFields());
		configBean.setArgumentTypes(getArgumentTypes());

		configBean.getArgumentFieldAliases().add(new ArrayList<>());
		configBean.getArgumentTypesByExpression().add(new ArrayList<>());
		configBean.getArgumentFieldsByExpression().add(new ArrayList<>());

		configBean.getExpressions().set(0,
		        ExpressionService.decodeExpression(configBean.getExpressions().get(0), configBean.getArgumentFieldsByExpression().get(0),
		                configBean.getArgumentTypesByExpression().get(0), configBean.getArgumentTypes(), configBean.getArgumentFieldAliases().get(0),
		                configBean.getArgumentFields()));

		return configBean;
	}

	private ArrayList<String> getExpressions() {
		ArrayList<String> expressions = new ArrayList<>();
		expressions.add("$0.equals($1)");

		return expressions;
	}

	private ArrayList<String> getArgumentFields() {
		ArrayList<String> argumentFields = new ArrayList<String>();
		argumentFields.add("a");
		argumentFields.add("b");

		return argumentFields;
	}

	private ArrayList<Class<?>> getArgumentTypes() {
		ArrayList<Class<?>> argumentTypes = new ArrayList<>();
		argumentTypes.add(String.class);
		argumentTypes.add(String.class);

		return argumentTypes;
	}

	@Override
	public LinkedHashMap<String, Object> getSourceValues() {
		LinkedHashMap<String, Object> sourceValues = new LinkedHashMap<>();
		sourceValues.put("a", "$asd");
		sourceValues.put("b", "$asd");
		sourceValues.put("c", 3);
		sourceValues.put("e", -3);
		sourceValues.put("f", -6);

		return sourceValues;
	}

	@Override
	public AnvizentFilterFunction getFunction() throws UnsupportedException, InvalidArgumentsException, InvalidRelationException {
		return new FilterByExpressionFunction((FilterByExpressionConfigBean) configBean, 0, structure, newStructure, null, null, null);
	}
}
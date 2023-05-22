package elt.core.spark.operation.function;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.function.AnvizentFunction;
import com.anvizent.elt.core.spark.operation.config.bean.ExpressionConfigBean;
import com.anvizent.elt.core.spark.operation.function.ExpressionFunction;
import com.anvizent.elt.core.spark.operation.service.ExpressionService;

import elt.core.spark.function.AnvizentFunctionPositiveCallTest;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ExpressionCallTest extends AnvizentFunctionPositiveCallTest {

	@Override
	public AnvizentFunction getFunction() throws UnsupportedException, InvalidArgumentsException, InvalidRelationException {

		ExpressionConfigBean expressionBean = (ExpressionConfigBean) configBean;
		ExpressionFunction expFunction = new ExpressionFunction(expressionBean, structure, newStructure, null, null, null);

		return expFunction;
	}

	@Override
	public ExpressionConfigBean getConfigBean() {
		ArrayList<Integer> expressionsFieldNameIndexes = new ArrayList<>();
		ArrayList<ArrayList<String>> tempDecodeArgsList = new ArrayList<ArrayList<String>>();
		ArrayList<ArrayList<Class<?>>> tempArgTypesList = new ArrayList<>();
		ArrayList<ArrayList<String>> tempArgFieldsList = new ArrayList<ArrayList<String>>();

		ExpressionConfigBean expressionConfigBean = new ExpressionConfigBean();

		expressionConfigBean.setExpressions(getExpressionsVal());
		expressionConfigBean.setArgumentFields(getArgumentFieldsVal());
		expressionConfigBean.setArgumentTypes(getArgumentTypesVal());
		expressionConfigBean.setReturnTypes(getReturnTypeVal());
		expressionConfigBean.setExpressionsFieldNames(getExpressionFieldNameVal());
		expressionConfigBean.setExpressionsFieldIndexes(expressionsFieldNameIndexes);

		expressionConfigBean.setArgumentFieldsByExpression(tempArgFieldsList);
		expressionConfigBean.setArgumentTypesByExpression(tempArgTypesList);
		expressionConfigBean.setArgumentFieldAliases(tempDecodeArgsList);
		ExpressionService.decodeExpressions(expressionConfigBean);

		return expressionConfigBean;
	}

	@Override
	public LinkedHashMap<String, Object> getExpectedValues() {
		LinkedHashMap<String, Object> expectedValues = new LinkedHashMap<>();

		expectedValues.put("Price", 270);
		expectedValues.put("Quantity", 2);
		expectedValues.put("Order", "$asd");
		expectedValues.put("Amount", 540);
		return expectedValues;
	}

	@Override
	public LinkedHashMap<String, Object> getSourceValues() {
		LinkedHashMap<String, Object> values = new LinkedHashMap<>();//
		values.put("Price", 270);
		values.put("Quantity", 2);
		values.put("Order", "$asd");

		return values;
	}

	private ArrayList<String> getExpressionsVal() {
		ArrayList<String> expressions = new ArrayList<>();
		expressions.add("$0 * $1 ");
		return expressions;
	}

	private ArrayList<String> getExpressionFieldNameVal() {
		ArrayList<String> expressionFieldName = new ArrayList<>();
		expressionFieldName.add("Amount");
		return expressionFieldName;
	}

	private ArrayList<String> getArgumentFieldsVal() {
		ArrayList<String> argumentFields = new ArrayList<String>();
		argumentFields.add("Price");
		argumentFields.add("Quantity");
		return argumentFields;
	}

	private ArrayList<Class<?>> getReturnTypeVal() {
		ArrayList<Class<?>> returnType = new ArrayList<>();
		returnType.add(Integer.class);
		return returnType;
	}

	private ArrayList<Class<?>> getArgumentTypesVal() {
		ArrayList<Class<?>> argumentTypes = new ArrayList<>();
		argumentTypes.add(Integer.class);
		argumentTypes.add(Integer.class);
		return argumentTypes;
	}

	@Override
	public void adjustNewStructure(LinkedHashMap<String, AnvizentDataType> newStructure) throws UnsupportedException {
		newStructure.put("Amount", new AnvizentDataType(Integer.class));
	}
}
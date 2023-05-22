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
public class ExpressionMultipleExpressionExceptionCallTest extends AnvizentFunctionPositiveCallTest {

	@Override
	public LinkedHashMap<String, Object> getSourceValues() {
		LinkedHashMap<String, Object> values = new LinkedHashMap<>();

		values.put("a", 2);
		values.put("b", 2);
		values.put("c", 3);
		values.put("h", 8);
		values.put("k", -3);
		values.put("l", -6);

		return values;
	}

	@Override
	public LinkedHashMap<String, Object> getExpectedValues() {
		LinkedHashMap<String, Object> expectedValues = new LinkedHashMap<>();

		expectedValues.put("a", 2);
		expectedValues.put("b", 2);
		expectedValues.put("c", 3);
		expectedValues.put("h", 8);
		expectedValues.put("k", -3);
		expectedValues.put("l", -6);
		expectedValues.put("d", 4);
		expectedValues.put("e", 0);

		return expectedValues;
	}

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

	private ArrayList<String> getExpressionsVal() {
		ArrayList<String> expressions = new ArrayList<>();
		expressions.add("$0 + $1 ");
		expressions.add("$2 / $3 ");
		return expressions;
	}

	private ArrayList<String> getExpressionFieldNameVal() {
		ArrayList<String> expressionFieldName = new ArrayList<>();
		expressionFieldName.add("d");
		expressionFieldName.add("e");
		return expressionFieldName;
	}

	private ArrayList<String> getArgumentFieldsVal() {
		ArrayList<String> argumentFields = new ArrayList<String>();
		argumentFields.add("a");
		argumentFields.add("b");
		argumentFields.add("c");
		argumentFields.add("h");
		return argumentFields;
	}

	private ArrayList<Class<?>> getReturnTypeVal() {
		ArrayList<Class<?>> returnType = new ArrayList<>();
		returnType.add(Integer.class);
		returnType.add(Integer.class);

		return returnType;
	}

	private ArrayList<Class<?>> getArgumentTypesVal() {
		ArrayList<Class<?>> argumentTypes = new ArrayList<>();

		argumentTypes.add(Integer.class);
		argumentTypes.add(Integer.class);
		argumentTypes.add(Integer.class);
		argumentTypes.add(Integer.class);

		return argumentTypes;
	}

	@Override
	public void adjustNewStructure(LinkedHashMap<String, AnvizentDataType> newStructure) throws UnsupportedException {
		newStructure.put("d", new AnvizentDataType(Integer.class));
		newStructure.put("e", new AnvizentDataType(Integer.class));
	}

}
package elt.core.spark.operation.function;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.codehaus.commons.compiler.CompileException;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.function.AnvizentFunction;
import com.anvizent.elt.core.spark.operation.config.bean.ExpressionConfigBean;
import com.anvizent.elt.core.spark.operation.function.ExpressionFunction;
import com.anvizent.elt.core.spark.operation.service.ExpressionService;

import elt.core.spark.function.AnvizentFunctionNegativeCallTest;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ExpressionReturnTypeMisMatchCallTest extends AnvizentFunctionNegativeCallTest {

	@Override
	public ConfigBean getConfigBean() {
		ArrayList<Integer> expressionsFieldNameIndexes = new ArrayList<>();
		ArrayList<ArrayList<String>> tempDecodeArgsList = new ArrayList<ArrayList<String>>();
		ArrayList<ArrayList<Class<?>>> tempArgTypesList = new ArrayList<>();
		ArrayList<ArrayList<String>> tempArgFieldsList = new ArrayList<ArrayList<String>>();

		ExpressionConfigBean expressionConfigBean = new ExpressionConfigBean();
		expressionConfigBean.setExpressions(getExpressions());
		expressionConfigBean.setArgumentFields(getArgumentFields());
		expressionConfigBean.setArgumentTypes(getArgumentTypes());
		expressionConfigBean.setReturnTypes(getReturnType());
		expressionConfigBean.setExpressionsFieldNames(getExpressionFieldName());
		expressionConfigBean.setExpressionsFieldIndexes(expressionsFieldNameIndexes);
		expressionConfigBean.setArgumentFieldsByExpression(tempArgFieldsList);
		expressionConfigBean.setArgumentTypesByExpression(tempArgTypesList);
		expressionConfigBean.setArgumentFieldAliases(tempDecodeArgsList);

		ExpressionService.decodeExpressions(expressionConfigBean);
		return expressionConfigBean;
	}

	@Override
	public LinkedHashMap<String, Object> getSourceValues() {
		LinkedHashMap<String, Object> values = new LinkedHashMap<>();
		values.put("Order", 2);
		values.put("Quantity", 2);
		values.put("Amount", 3);
		values.put("TotalCost", -3);
		return values;
	}

	@Override
	public Class<? extends Throwable> getExpectedException() {
		return CompileException.class;
	}

	@Override
	public int getCauseDepth() {
		return 1;
	}

	@Override
	public AnvizentFunction getFunction() throws UnsupportedException, InvalidArgumentsException, InvalidRelationException {
		ExpressionConfigBean expressionBean = (ExpressionConfigBean) configBean;
		ExpressionFunction expFunction = new ExpressionFunction(expressionBean, structure, newStructure, null, null, null);
		return expFunction;
	}

	private ArrayList<String> getExpressions() {
		ArrayList<String> expressions = new ArrayList<>();
		expressions.add("$0 + $1 ");
		return expressions;
	}

	private ArrayList<String> getExpressionFieldName() {
		ArrayList<String> expressionFieldName = new ArrayList<>();
		expressionFieldName.add("Price");
		return expressionFieldName;
	}

	private ArrayList<String> getArgumentFields() {
		ArrayList<String> argumentFields = new ArrayList<String>();
		argumentFields.add("Order");
		argumentFields.add("Quantity");
		return argumentFields;
	}

	private ArrayList<Class<?>> getReturnType() {
		ArrayList<Class<?>> returnType = new ArrayList<>();
		returnType.add(String.class);
		return returnType;
	}

	private ArrayList<Class<?>> getArgumentTypes() {
		ArrayList<Class<?>> argumentTypes = new ArrayList<>();

		argumentTypes.add(Integer.class);
		argumentTypes.add(Integer.class);

		return argumentTypes;
	}

	@Override
	public void adjustNewStructure(LinkedHashMap<String, AnvizentDataType> newStructure) throws UnsupportedException {
		newStructure.put("Price", new AnvizentDataType(String.class));
	}
}
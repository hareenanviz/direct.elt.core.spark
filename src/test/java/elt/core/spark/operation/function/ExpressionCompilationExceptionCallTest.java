package elt.core.spark.operation.function;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.codehaus.commons.compiler.CompileException;

import com.anvizent.elt.core.lib.AnvizentDataType;
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
public class ExpressionCompilationExceptionCallTest extends AnvizentFunctionNegativeCallTest {

	@Override
	public ExpressionConfigBean getConfigBean() {

		ArrayList<ArrayList<String>> tempDecodeArgsList = new ArrayList<ArrayList<String>>();
		ArrayList<ArrayList<Class<?>>> tempArgTypesList = new ArrayList<>();
		ArrayList<ArrayList<String>> tempArgFieldsList = new ArrayList<ArrayList<String>>();
		ArrayList<Integer> expressionsFieldNameIndexes = new ArrayList<>();

		ExpressionConfigBean expressionBean = new ExpressionConfigBean();
		expressionBean.setExpressions(getExpressionsVal());
		expressionBean.setArgumentFields(getArgumentFieldsVal());
		expressionBean.setArgumentTypes(getArgumentTypesVal());
		expressionBean.setReturnTypes(getReturnTypeVal());
		expressionBean.setExpressionsFieldNames(getExpressionFieldNameVal());
		expressionBean.setExpressionsFieldIndexes(expressionsFieldNameIndexes);
		expressionBean.setArgumentFieldsByExpression(tempArgFieldsList);
		expressionBean.setArgumentTypesByExpression(tempArgTypesList);
		expressionBean.setArgumentFieldAliases(tempDecodeArgsList);

		ExpressionService.decodeExpressions(expressionBean);

		return expressionBean;

	}

	@Override
	public LinkedHashMap<String, Object> getSourceValues() {
		LinkedHashMap<String, Object> values = new LinkedHashMap<>();

		values.put("a", 2);
		values.put("b", 2);
		values.put("c", 3);
		values.put("e", -3);
		values.put("f", -6);

		return values;
	}

	@Override
	public AnvizentFunction getFunction() throws UnsupportedException, InvalidArgumentsException, InvalidRelationException {
		ExpressionConfigBean expressionBean = (ExpressionConfigBean) configBean;
		ExpressionFunction expFunction = new ExpressionFunction(expressionBean, structure, newStructure, null, null, null);
		return expFunction;
	}

	@Override
	public Class<? extends Throwable> getExpectedException() {
		return CompileException.class;
	}

	@Override
	public int getCauseDepth() {
		return 1;
	}

	private ArrayList<String> getExpressionsVal() {
		ArrayList<String> expressions = new ArrayList<>();
		expressions.add("abc +  $1 ");
		return expressions;
	}

	private ArrayList<String> getExpressionFieldNameVal() {
		ArrayList<String> expressionFieldName = new ArrayList<>();
		expressionFieldName.add("d");
		return expressionFieldName;
	}

	private ArrayList<String> getArgumentFieldsVal() {
		ArrayList<String> argumentFields = new ArrayList<String>();
		argumentFields.add("a");
		argumentFields.add("b");
		return argumentFields;
	}

	private ArrayList<Class<?>> getReturnTypeVal() {
		ArrayList<Class<?>> returnType = new ArrayList<>();
		returnType.add(Integer.class);
		return returnType;
	}

	private ArrayList<Class<?>> getArgumentTypesVal() {
		ArrayList<Class<?>> argumentTypes = new ArrayList<>();
		argumentTypes.add(String.class);
		argumentTypes.add(Integer.class);
		return argumentTypes;
	}

	@Override
	public void adjustNewStructure(LinkedHashMap<String, AnvizentDataType> newStructure) throws UnsupportedException {
		newStructure.put("d", new AnvizentDataType(Integer.class));
	}

}
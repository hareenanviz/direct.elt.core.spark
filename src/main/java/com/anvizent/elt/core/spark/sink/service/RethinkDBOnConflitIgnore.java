package com.anvizent.elt.core.spark.sink.service;

import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction3;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RethinkDBOnConflitIgnore implements ReqlFunction3 {

	@Override
	public Object apply(ReqlExpr id, ReqlExpr oldDoc, ReqlExpr newDoc) {
		return oldDoc;
	}

}

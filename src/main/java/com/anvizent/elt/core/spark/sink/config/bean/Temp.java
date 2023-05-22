package com.anvizent.elt.core.spark.sink.config.bean;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Temp {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {

		Class.forName("com.mysql.cj.jdbc.Driver");
		Connection connection = DriverManager.getConnection("jdbc:mysql://192.168.0.124:4475/MigSchedul_1010611_appdb", "almadmin", "Ewc@4fvQ#pT5");
		Connection connection1 = DriverManager.getConnection("jdbc:mysql://192.168.0.124:4475/MigSchedul_1010611_appdb", "almadmin", "Ewc@4fvQ#pT5");

		connection.setAutoCommit(false);
		connection1.setAutoCommit(false);
		Statement statement = connection.createStatement();
		statement.execute("insert into temp values (6 , 'kamal')");
		Statement statement1 = connection1.createStatement();
		statement1.execute("delete from temp where id = 6");
		connection.commit();
		connection1.commit();

		statement.close();
		statement1.close();
		connection.close();
		connection1.close();
	}

}

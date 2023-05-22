package elt.core.spark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.anvizent.elt.core.spark.AnvizentDate;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class AnvizentDateTest {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
//		Connection connection = getConnectionForConnectorID("com.mysql.jdbc.Driver", "jdbc:mysql://192.168.0.135:4475/", "almadmin", "Un!qu3Pa5$");
//		PreparedStatement selectStatement = getSelectStatement(connection);
//		String timeString = "2017-08-15 18:00:00";
//		
////		SimpleDateFormat
//		
//		String expectedTime = "2017-08-15 08:30:00";
	}

	public static Connection getConnectionForConnectorID(String driver, String jdbcURL, String userName, String password)
			throws SQLException, ClassNotFoundException {
		Class.forName(driver);
		Connection connection = DriverManager.getConnection(jdbcURL, userName, password);

		return connection;
	}

	public static void get(Connection connection, PreparedStatement selectStatement, AnvizentDate date)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		setParams(selectStatement, date);
		ResultSet resultSet = selectStatement.executeQuery();

		while (resultSet.next()) {
			System.out.println("id: " + resultSet.getLong(1) + ", date: " + resultSet.getObject(2));
		}

		resultSet.close();
	}

	public static PreparedStatement getSelectStatement(Connection connection) throws SQLException {
		String sql = "SELECT id, datetime_col FROM `all_types` WHERE `timestamp_col`=?";

		return connection.prepareStatement(sql);
	}

	private static void setParams(PreparedStatement preparedStatement, Object... values) throws SQLException {
		if (values != null) {
			for (int i = 0; i < values.length; i++) {
				preparedStatement.setObject(i + 1, values[i]);
			}
		}
	}

	public static void closeAll(Connection connection, PreparedStatement... statements) throws SQLException {
		for (PreparedStatement statement : statements) {
			statement.close();
		}

		if (connection != null) {
			connection.close();
		}
	}
}
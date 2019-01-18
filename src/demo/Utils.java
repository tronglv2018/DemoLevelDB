package demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Utils {

	private static long currentNumber = 800000000;//200 trieu so thue bao isdn
	public static final long minNumber = 800000000;
	public static final long maxNumber = 999999999;
	public static final int batchSize = 250000;
	public static final int qtyIsdnSearch = 100000;

	private static final String MYSQL_DB_URL = "jdbc:mysql://localhost:3306/mydb";
	private static final String MYSQL_USER_NAME = "root";
	private static final String MYSQL_PASSWORD = "123456a@";
	public static final String MYSQL_INSERT_STRING = "insert into subscriber (isdn, status, create_username) values (?, ?, ?)";
	public static final String MYSQL_SELECT_STRING = "select status from subscriber where isdn = ? ";

	public static synchronized long getCurrentNumber() {
		return currentNumber++;
	}

	public static synchronized boolean isDone() {
		return (currentNumber > maxNumber);
	}

	public static long randomIsdn() {
		return randomWithRange(minNumber, maxNumber);
	}

	public static long randomWithRange(long min, long max) {
		long range = (max - min) + 1;
		return (long) (Math.random() * range) + min;
	}

	public static int randomWithRange(int min, int max) {
		int range = (max - min) + 1;
		return (int) (Math.random() * range) + min;
	}

	public static Connection getConnMySql() {
		return getConnMySql(MYSQL_DB_URL, MYSQL_USER_NAME, MYSQL_PASSWORD);
	}

	public static Connection getConnMySql(String dbURL, String userName, String password) {
		try {
			return DriverManager.getConnection(dbURL, userName, password);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

}

package demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MySqlMain {
	
	
	public static long currentNumber = 960000000;
	public static final long minNumber = 960000000;
	public static final long maxNumber = 999999999;
	public static final int batchSize = 100000;//batch = 100.000
	public static final int qtyIsdnSearch = 200000;// = 200.000
	
	public static final int qtyThreads= 4;
	public static final int queueSize= 10;
	
	private static final String MYSQL_DB_URL = "jdbc:mysql://localhost:3306/mydb";
	private static final String MYSQL_USER_NAME = "root";
	private static final String MYSQL_PASSWORD = "123456a@";
	public static final String MYSQL_INSERT_STRING = "insert into subscriber (isdn, productid, create_username, status) values (?, ?, ?, ?)";
	public static final String MYSQL_SELECT_STRING = "select status from subscriber where isdn = ?  ";
	public static final String MYSQL_PROCUCT_CODE_LIST = "('code5','code10','code15')";



	public static void main(String[] args) {
		Date d ;
		long startTime ;
		long endTime ;
		
		// 1. Truncate Table
		truncateDb();

		// 2. Insert Data
//		d = new Date();
//		System.out.println("Starting threads to insert data. At " + d.getHours() + ":" + d.getMinutes() + ":" + d.getSeconds());
//
//		startTime = System.currentTimeMillis();
//		ExecutorService executor = Executors.newFixedThreadPool(Utils.queueSize);
//		for (int i = 1; i <= Utils.qtyThreads; i++) {
//			Runnable worker = new MySqlThread("Job_" + i);
//			executor.execute(worker);
//		}
//		executor.shutdown();
//		while (!executor.isTerminated()) {
//		}
//		endTime = System.currentTimeMillis();
//		System.out.println("\n\n\nFinished all threads. Total insert time = " + (endTime - startTime) / 1000 + " s");

		// 3. Query Data
		d = new Date();
		System.out.println("\n\n\nStarting Query data. At " + d.getHours() + ":" + d.getMinutes() + ":" + d.getSeconds());
		startTime = System.currentTimeMillis();
		queryDb();
		endTime = System.currentTimeMillis();
		System.out.println("Finished. Total Query data time = " + (endTime - startTime) / 1000 + " s");

	}

	static void truncateDb() {
		Connection conn = null;
		Statement statement = null;
		try {
			conn = getConnMySql();
			if (conn == null)
				return;
			statement = conn.createStatement();
			statement.executeUpdate("TRUNCATE table subscriber ");
			System.out.println("Successfully truncated subscriber");
			statement.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (statement != null)
					statement.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	static void queryDb() {
		// Tao mang 200.000 so de test tong thoi gian select
		Long[] isdnList = new Long[qtyIsdnSearch];
		String[] productList = new String[qtyIsdnSearch];
		for (int i = 0; i < qtyIsdnSearch; i++) {
			isdnList[i] = Utils.randomWithRange(minNumber, maxNumber);
		}

		Connection conn = null;
		Statement stmt = null;
//		PreparedStatement ps = null;
		
		try {
			conn = getConnMySql();
			if (conn == null)
				return;
			
			int qtyExistNumber = 0; 
			for (int i = 0; i < qtyIsdnSearch; i++) {
				// add batch
				// ps = conn.prepareStatement(Utils.MYSQL_SELECT_STRING);
				// ps.setLong(1, isdnList[i]);

				stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(
						"select b.code from mydb.subscriber a join mydb.product b on a.productid = b.id where a.isdn = "
								+ isdnList[i] + " and a.status= 2  and b.code in " + MYSQL_PROCUCT_CODE_LIST);
				while (rs.next()) {
					qtyExistNumber++;
					productList[i] = rs.getString(1);
					break;
				}

				if (i>0 && i % 5000 == 0) {
					System.out.println("Da query " + i + " number. Co " + qtyExistNumber + " number thoa man dk status = 2 and product in (code5,code10,code15) ");
				}
				
			}

			stmt.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static synchronized long getIncreaseNumber() {
		return currentNumber++;
	}

	public static synchronized boolean isDone() {
		return (currentNumber > maxNumber);
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
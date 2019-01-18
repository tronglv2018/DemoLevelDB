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

	public static void main(String[] args) {
		// 1. Truncate Table
		truncateDb();

		// 2. Insert Data
		Date d = new Date();
		System.out
				.println("Start threads to insert data. " + d.getHours() + ":" + d.getMinutes() + ":" + d.getSeconds());

		long startTime = System.currentTimeMillis();
//		ExecutorService executor = Executors.newFixedThreadPool(20);
//		for (int i = 1; i <= 12; i++) {
//			Runnable worker = new MySqlThread("Job_" + i);
//			executor.execute(worker);
//		}
//		executor.shutdown();
//		while (!executor.isTerminated()) {
//		}
		long endTime = System.currentTimeMillis();
		System.out.println("Finished all threads. Total insert time = " + (endTime - startTime) / 1000 + " s");

		// 3. Find Data
		d = new Date();
		System.out.println("Find data. " + d.getHours() + ":" + d.getMinutes() + ":" + d.getSeconds());
		startTime = System.currentTimeMillis();
		findDb();
		endTime = System.currentTimeMillis();
		System.out.println("Finished. Total find data time = " + (endTime - startTime) / 1000 + " s");

	}

	static void truncateDb() {
		Connection conn = null;
		Statement statement = null;
		try {
			conn = Utils.getConnMySql();
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

	static void findDb() {
		// Tao mang 100.000 so de test tong thoi gian select
		Long[] isdnList = new Long[Utils.qtyIsdnSearch];
		Integer[] statusList = new Integer[Utils.qtyIsdnSearch];
		for (int i = 0; i < Utils.qtyIsdnSearch; i++) {
			isdnList[i] = Utils.randomIsdn();
		}

		Connection conn = null;
		Statement stmt = null;
//		PreparedStatement ps = null;
		try {
			conn = Utils.getConnMySql();
			if (conn == null)
				return;
			for (int i = 0; i < Utils.qtyIsdnSearch; i++) {
				// add batch
				// ps = conn.prepareStatement(Utils.MYSQL_SELECT_STRING);
				// ps.setLong(1, subsList[i][0]);

				stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(Utils.MYSQL_SELECT_STRING);
				while (rs.next()) {
					statusList[i] = rs.getInt(1);
					break;
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

}
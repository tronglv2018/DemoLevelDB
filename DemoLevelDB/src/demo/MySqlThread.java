package demo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

public class MySqlThread implements Runnable {
	private String name = "";

	public MySqlThread(String name) {
		this.name = name;
	}

	@Override
	public void run() {
		println(Thread.currentThread().getName() + " Start. Name = " + name);
		insertMySql();
		println(Thread.currentThread().getName() + " End.");
	}

	@Override
	public String toString() {
		return this.name;
	}

	void insertMySql() {
		Connection conn = null;
		PreparedStatement ps = null;
		long start = 0;

		try {
			conn = MySqlMain.getConnMySql();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(MySqlMain.MYSQL_INSERT_STRING);
			start = System.currentTimeMillis();
			println("------------------------------Start Thread------------------------------");
			long count = 0;
			while (!MySqlMain.isDone()) {
				long num = MySqlMain.getIncreaseNumber();
				ps.setLong(1, num);
				ps.setLong(2, Utils.randomWithRange(1, 20));//productid in 1-20
				ps.setString(3, Thread.currentThread().getName() + " : " + this.name);
				ps.setLong(4, Utils.randomWithRange(1, 3));//status in 0-3
				ps.addBatch();

				if (++count % MySqlMain.batchSize == 0) {
					int[] result = ps.executeBatch();
					conn.commit();
					println("Commit Batch. Qty number of thread: " + count + ". Time taken: " + (System.currentTimeMillis() - start) / 1000 + " s. Current number: " + num);
				}
			}
			int[] result = ps.executeBatch();
			conn.commit();
			ps.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (ps != null)
					ps.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		println("Finish Thread. Time Taken = " + (System.currentTimeMillis() - start) / 1000 + " s");
	}

	void println(String s) {
		Date d = new Date();
		System.out.println("[" + Thread.currentThread().getName() + "].[" + this.name + "].[" + d.getHours() + ":" + d.getMinutes() + ":" + d.getSeconds() + "].[" + s + "]");
	}
}
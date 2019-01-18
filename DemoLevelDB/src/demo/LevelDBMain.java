package demo;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;

public class LevelDBMain {

	public static void main(String[] args) throws Exception {
		// 1. Insert db voi 2 ti ban ghi
		putDb();

		// 2. Search 100.000 ban ghi
		getDb();
	}

	static void putDb() {
		Options options = new Options();
		options.createIfMissing(true);
		DB db = null;
		try {
			LocalDateTime startDate = LocalDateTime.now();

			System.out.println("START PUT: " + startDate.toString());
			db = factory.open(new File("example"), options);
			WriteBatch batch = db.createWriteBatch();
			long qty = 0;
			int batchno = 0;
			for (long i = Utils.minNumber; i <= Utils.maxNumber; i++) {
				qty++;
				batch.put(bytes("" + i), bytes("" + Utils.randomWithRange(0, 3)));
				if (qty % Utils.batchSize == 0) {
					db.write(batch);
					db.close();
					LocalDateTime tmp = LocalDateTime.now();
					batchno++;
					System.out.println("Batch: " + batchno + "(" + qty + " so) | " + tmp.toString() + "("
							+ Duration.between(startDate, tmp).toMillis() / 1000 + "giay)");
					db = null;
					batch = null;
					db = factory.open(new File("example"), options);
					batch = db.createWriteBatch();
				}
			}
			// Ghi cac records con lai
			db.write(batch);

			LocalDateTime endDate = LocalDateTime.now();
			System.out.println("START PUT DB: " + startDate.toString());
			System.out.println("END PUT DB: " + endDate.toString());
			System.out.println("=====> TOTAL DURATION TO PUT DB: "
					+ Duration.between(startDate, endDate).toMillis() / 1000 + " s");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (db != null)
					db.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	static void getDb() {
		String[] keyList = new String[Utils.qtyIsdnSearch];
		String[] valueList = new String[Utils.qtyIsdnSearch];

		for (int i = 0; i < Utils.qtyIsdnSearch; i++) {
			Long num = Utils.randomIsdn();
			keyList[i] = num.toString();
		}

		LocalDateTime startDate = LocalDateTime.now();
		System.out.println("START: " + startDate.toString());

		Options options = new Options();
		options.createIfMissing(true);
		DB db = null;
		try {
			db = factory.open(new File("example"), options);
			for (int i = 0; i < Utils.qtyIsdnSearch; i++) {
				valueList[i] = getDbByKey(db, keyList[i]);
			}
			db.close();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (db != null)
					db.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		LocalDateTime endDate = LocalDateTime.now();

		for (int i = 0; i < Utils.qtyIsdnSearch; i++) {
			System.out.println("key:= " + keyList[i] + " ; value:= " + valueList[i]);
		}

		System.out.println("START GET DB: " + startDate.toString());
		System.out.println("END GET DB: " + endDate.toString());
		System.out.println(
				"=====> TOTAL DURATION TO GET DB: " + Duration.between(startDate, endDate).toMillis() / 1000 + " s");
	}

	static String getDbByKey(DB db, String key) throws Exception {
		byte[] bytes = db.get(bytes(key));
		if (bytes != null)
			return new String(bytes);
		return null;
	}

}

package demo;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import com.sun.org.apache.xml.internal.security.keys.content.KeyValue;

import static org.iq80.leveldb.impl.Iq80DBFactory.*;

public class LevelDBMain {

	//LEVELDB
	public static final int LEVELDB_minNumber = 900000000;//total = 100.000.000 so 
	public static final int LEVELDB_maxNumber = 999999999;
	public static final int LEVELDB_batchSize = 500000;//batch = 500.000 so
	public static final int LEVELDB_qtyIsdnSearch = 1000000;//get = 1.000.000 so
	public static final String DB_SUBSCRIBER_NAME= "dbSubscriber";
	public static final String DB_SUB_PRODUCT_NAME= "dbSubProduct";
	
	public static void main(String[] args) throws Exception {
		// 1. Insert to DB with 100.000.000 records
		LocalDateTime startDate = LocalDateTime.now();
		System.out.println("Start put DB at " + startDate.getHour() + ":" + startDate.getMinute() + ":" + startDate.getSecond());
		//
		putDb();

		LocalDateTime endDate = LocalDateTime.now();		
		System.out.println("End put DB at " + endDate.getHour() + ":" + endDate.getMinute() + ":" + endDate.getSecond());
		
		System.out.println("Total number to put DB: " + (LEVELDB_maxNumber - LEVELDB_minNumber + 1) + " number");
		System.out.println("Taken total duration time: " + Duration.between(startDate, endDate).toMillis() / 1000 + " s");
		
		
		// 2. Query from DB with 1.000.000 records
		startDate = LocalDateTime.now();
		System.out.println("\nStart get DB at " + startDate.getHour() + ":" + startDate.getMinute() + ":" + startDate.getSecond());
		//
		getDb();
		
		endDate = LocalDateTime.now();
		System.out.println("End get DB at " + endDate.getHour() + ":" + endDate.getMinute() + ":" + endDate.getSecond());
		
		System.out.println("Total number to get DB: " + LEVELDB_qtyIsdnSearch + " number");
		System.out.println("Taken total duration time: " + Duration.between(startDate, endDate).toMillis() / 1000 + " s");
		
	}
	

	/**
	 * Put data to DB
	 * Method:
	 * 		random status: 0->3
	 * 		random product_id: 1->20
	 * 		Put 100.000.000 number (900.000.000->999.999.999) & status (random in 0,1,2,3) to DB1_SUBSCRIBER
	 * 		Put 100.000.000 number (900.000.000->999.999.999) & product_id (random in 1,2,3,...,18,19,20) to DB2_SUB_PRODUCT
	 * 		Put by batch, with batch size = 500.000 number
	 */
	static void putDb() {
		Options options = new Options();
		options.createIfMissing(true);
		DB db = null;
		DB dbSubProduct = null;
		
		try {
			LocalDateTime startDate = LocalDateTime.now();
			
			db = factory.open(new File(DB_SUBSCRIBER_NAME), options);
			dbSubProduct = factory.open(new File(DB_SUB_PRODUCT_NAME), options);
			
			WriteBatch batch = db.createWriteBatch();
			WriteBatch batchDbSubProduct = dbSubProduct.createWriteBatch();
			
			long qty = 0;
			int batchno = 0;
			for (int i = LEVELDB_minNumber; i <= LEVELDB_maxNumber; i++) {
				qty++;
				//sub_status
				batch.put(bytes("" + i), bytes("" + Utils.randomWithRange(0, 3)));
				
				//sub_product
				batchDbSubProduct.put(bytes("" + i), bytes("" + Utils.randomWithRange(1, 20)));
				
				if (qty % LEVELDB_batchSize == 0) {
					db.write(batch);
					db.close();
					
					dbSubProduct.write(batchDbSubProduct);
					dbSubProduct.close();
					batchno++;
					
					LocalDateTime tmp = LocalDateTime.now();					
					System.out.println("Put batch no:= " + batchno + " (putted " + qty + " number). At " + tmp.getHour() + ":" + tmp.getMinute() + ":" + tmp.getSecond() + ". Taken time:= " + Duration.between(startDate, tmp).toMillis() / 1000 + " s)");
					
					db = null;
					batch = null;
					db = factory.open(new File(DB_SUBSCRIBER_NAME), options);
					batch = db.createWriteBatch();
					
					dbSubProduct = null;
					batchDbSubProduct = null;
					dbSubProduct = factory.open(new File(DB_SUB_PRODUCT_NAME), options);
					batchDbSubProduct = dbSubProduct.createWriteBatch();
				}
			}
			// Ghi cac records con lai
			db.write(batch);
			dbSubProduct.write(batchDbSubProduct);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (db != null)
					db.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				if (dbSubProduct != null)
					dbSubProduct.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}

	/**
	 * Get data from DB
	 * DB:
	 * 		subs number: 900.000.000 -> 999.999.999 
	 * 		status: 0,1,2,3
	 * 		product_id: 1,2,3,...,18,19,20
	 * Method:
	 * 		random 1.000.000 subs number
	 * 		query step1: get subs with subs.status=2 from DB1_SUBSCRIBER
	 * 		query step2: get subs.product_id in (5,10,15) from DB2_SUB_PRODUCT
	 */
	static void getDb() {
		String[] keyList = new String[LEVELDB_qtyIsdnSearch];
		HashMap<String, String> subProductList = new HashMap<String, String>();

		for (int i = 0; i < LEVELDB_qtyIsdnSearch; i++) {
			Integer num = Utils.randomWithRange(LEVELDB_minNumber, LEVELDB_maxNumber);//random number between min_number and max_number
			keyList[i] = num.toString();
		}
		
		Options options = new Options();
		options.createIfMissing(true);
		DB db = null;
		DB dbSubProduct = null;
		long qtyNumberActive = 0;
		long qtyNumberActiveProductCode = 0;
		try {
			db = factory.open(new File(DB_SUBSCRIBER_NAME), options);
			dbSubProduct = factory.open(new File(DB_SUB_PRODUCT_NAME), options);
			
			for (int i = 0; i < LEVELDB_qtyIsdnSearch; i++) {
				String tmp = getDbByKey(db, keyList[i]);

				// Check goi cuoc neu nam trong danh sach thi dua vao list
				if (tmp != null && Utils.ISDN_STATUS_ACTIVE.equals(tmp)) {
					qtyNumberActive++;
					String tmp2 = getDbByKey(dbSubProduct, keyList[i]);
					
					if (tmp2 != null && Utils.PROCUCT_ID_LIST.indexOf("'"+tmp2+"'") >= 0) {
						qtyNumberActiveProductCode++;
						subProductList.put(keyList[i], tmp2);
					}
				}
			}
			
			System.out.println("Done. So luong ban ghi thoa man dieu kien status = 2: " +qtyNumberActive + " so");			
			System.out.println("Done. So luong ban ghi thoa man dieu kien status = 2 and product in (code5,code10,code15): "+ subProductList.size() + " so");
			db.close();
			dbSubProduct.close();

		} catch (Exception e) {
			try {
				if (db != null)
					db.close();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			try {
				if (dbSubProduct != null)
					dbSubProduct.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
			e.printStackTrace();
		} 
	}

	static String getDbByKey(DB db, String key) throws Exception {
		byte[] bytes = db.get(bytes(key));
		if (bytes != null)
			return new String(bytes);
		return null;
	}

}

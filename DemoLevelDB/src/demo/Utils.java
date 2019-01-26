package demo;


public class Utils {
	

	public static final String PROCUCT_ID_LIST = "('5','10','15')";
	public static final String ISDN_STATUS_ACTIVE = "2";
	
	public static long randomWithRange(long min, long max) {
		long range = (max - min) + 1;
		return (long) (Math.random() * range) + min;
	}

	public static int randomWithRange(int min, int max) {
		int range = (max - min) + 1;
		return (int) (Math.random() * range) + min;
	}

	
}

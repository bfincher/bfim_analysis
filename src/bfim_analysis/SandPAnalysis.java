package bfim_analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class SandPAnalysis implements Runnable {

	private static final float INITIAL_INVESTMENT = 1000.0f;

	private static class SandPEntry {
		public long date;
		public float close;

		public String toString() {
			return String.valueOf(date) + " " + String.valueOf(close);
		}
	}

	private static DecimalFormat decimalFormat = new DecimalFormat("#########0.##");

	private static class CalculateGainResult implements Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public ArrayList<Integer> buyDates;
		public ArrayList<Integer> sellDates;
		public ArrayList<Float> balances;
		public float finalBalance;

		private void writeObject(ObjectOutputStream out) throws IOException {
			out.writeInt(buyDates.size());
			for (Integer i: buyDates) {
				out.writeInt(i);
			}

			out.writeInt(sellDates.size());
			for (Integer i: sellDates) {
				out.writeInt(i);
			}

			out.writeInt(balances.size());
			for (Float f: balances) {
				out.writeFloat(f);
			}

			out.writeFloat(finalBalance);
		}

		private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
			int size = in.readInt();
			buyDates = new ArrayList<Integer>(size);
			for (int i = 0; i < size; i++) {
				buyDates.add(in.readInt());
			}

			size = in.readInt();
			sellDates = new ArrayList<Integer>(size);
			for (int i = 0; i < size; i++) {
				sellDates.add(in.readInt());
			}

			size = in.readInt();
			balances = new ArrayList<Float>(size);
			for (int i = 0; i < size; i++) {
				balances.add(in.readFloat());
			}

			finalBalance = in.readFloat();
		}

		public String toString() {
			StringBuffer sb = new StringBuffer();

			//			for (int i = 0; i < buyDates.size(); i++) {
			//				Date buyDate = new Date(sandP[buyDates.get(i)].date);
			//				Date sellDate = new Date(sandP[sellDates.get(i)].date);
			//				sb.append("Buy Date: " + sdf.format(buyDate) +
			//						" Sell Date: " + sdf.format(sellDate) +
			//						" Balance = " + decimalFormat.format(balances.get(i)));
			//				sb.append("\n");
			//			}
			sb.append("Final balance = " + decimalFormat.format(finalBalance));

			return sb.toString();
		}
	}

	private static class FinalResult implements Comparable<FinalResult>, Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public float sellCriteria;
		public float buyCriteria;
		public float balance;
		public int sellDays;
		public int buyDays;
		public CalculateGainResult details;

		public FinalResult(float sellCriteria, float buyCriteria, float balance,
				int sellDays, int buyDays, CalculateGainResult details) {
			this.buyCriteria = buyCriteria;
			this.sellCriteria = sellCriteria;
			this.balance = balance;
			this.sellDays = sellDays;
			this.buyDays = buyDays;
			this.details = details;
		}		

		public int compareTo(FinalResult result) {
			float delta = balance - result.balance;
			if (Math.abs(delta) < 0.0001)
				return 0;
			else if (balance < result.balance)
				return -1;
			else
				return 1;
		}

		public String toString() {
			StringBuilder sb = new StringBuilder("Sell Criteria = " + sellCriteria + ", buyCriteria = " + buyCriteria + ", sellDays = " + sellDays + ", buyDays = " + buyDays + ", balance = " + balance + "\n");
			sb.append(details.toString());
			return sb.toString();
		}
	}

	private static SandPEntry[] sandP;	

	static ArrayBlockingQueue<FinalResult> results = new ArrayBlockingQueue<FinalResult>(100);


	static SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

	int lowerIndex;
	int upperIndex;
	float sellCriteria;
	float buyCriteria;
	int sellDays;
	int buyDays;

	public SandPAnalysis(int lowerIndex, int upperIndex, float sellCriteria,
			float buyCriteria, int sellDays, int buyDays) {
		this.lowerIndex = lowerIndex;
		this.upperIndex = upperIndex;
		this.sellCriteria = sellCriteria;
		this.buyCriteria = buyCriteria;
		this.sellDays = sellDays;
		this.buyDays = buyDays;
	}

	public void run() {
		float[] sellReturns = getReturnSincePeriod(sellDays);
		float[] buyReturns = getReturnSincePeriod(buyDays);

		CalculateGainResult cgr = calculateGain(lowerIndex, upperIndex, sellCriteria, buyCriteria, sellReturns, buyReturns);
		FinalResult fr = new FinalResult(sellCriteria, buyCriteria, cgr.finalBalance, sellDays, buyDays, cgr);
		if (results.size() == 0)
			results.add(fr);
		else {
			switch(fr.compareTo(results.peek())) {
			case 0:
				results.offer(fr);
				break;

			case 1:
				results.clear();
				results.offer(fr);
				break;

			}
		}

	}

	private static void loadData(String fileName) throws Exception {				

		ArrayList<SandPEntry> list = new ArrayList<SandPEntry>(14700);
		BufferedReader input = null;
		try {
			input = new BufferedReader(new FileReader(fileName));
			String str;

			while ((str = input.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(str, ",");
				String dateStr = st.nextToken();
				SandPEntry entry = new SandPEntry();
				entry.date = sdf.parse(dateStr).getTime();

				entry.close = Float.parseFloat(st.nextToken());
				list.add(entry);
			}

			sandP = new SandPEntry[list.size()];
			sandP = list.toArray(sandP);
		} finally {
			if (input != null) {
				input.close();
			}
		}
	}

	private static class ReturnsSince implements Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		//		public int days;
		public float[] returns;
	}

	private static Map<Integer, ReturnsSince> returnMap = new HashMap<Integer, ReturnsSince>();

	private static void calculateReturnsSincePeriod(int lower, int upper) throws Exception {
		for (int i = lower; i <= upper; i++) {
			File file = new File("data/Return_" + i + ".dat");

			boolean readFromFile = false;

			if (file.exists()) {
				ObjectInputStream input = new ObjectInputStream(new FileInputStream(file));
				ReturnsSince rs = (ReturnsSince)input.readObject();
				input.close();
				if (rs.returns.length == sandP.length) {
					readFromFile = true;
					returnMap.put(i, rs);
				}
				else {
					file.delete();
					readFromFile = false;
				}
			}

			if (!readFromFile) {
				ReturnsSince rs = new ReturnsSince();
				//				rs.days = i;
				rs.returns = calculateReturnSincePeriod(i);
				ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(file));
				output.writeObject(rs);
				output.close();
				returnMap.put(i, rs);
			}
		}
	}

	private static final float[] getReturnSincePeriod(int days) {
		return returnMap.get(days).returns;
	}

	private static float[] calculateReturnSincePeriod(int days) {

		float[] returns = new float[sandP.length];
		for (int i = days; i < returns.length; i++) {
			float pointsGain = sandP[i].close - sandP[i-days].close;
			returns[i] = pointsGain / sandP[i-days].close;
		}		

		return returns;
	}

	private static int[] getDateIndexRange(long lowerDate, long upperDate) {
		int lowerIndex = 0;
		int upperIndex = sandP.length - 1;

		for (int i = 0; i < sandP.length; i++) {
			if (lowerDate <= sandP[i].date) {
				lowerIndex = i;
				break;
			}
		}

		for (int i = lowerIndex; i < sandP.length; i++) {
			if (upperDate <= sandP[i].date) {
				upperIndex = i;
				break;
			}
		}

		int[] returnArray = {lowerIndex, upperIndex};
		return returnArray;
	}

	private static enum StateEnum {looking_to_sell, looking_to_buy};

	private static final float calculateGain(int lowerIndex, int upperIndex) {
		float delta = sandP[upperIndex].close - sandP[lowerIndex].close;
		float gain = delta / sandP[lowerIndex].close;
		return gain;
	}

	private static final float calculateGainDollars(float initialInv, int lower, int upper) {
		float gainPct = calculateGain(lower, upper);
		float gainDollars = initialInv * gainPct;
		return initialInv + gainDollars;		
	}

	private static CalculateGainResult calculateGain(int lowerIndex, int upperIndex,  
			float sellCriteria, float buyCriteria, float[] sellReturns, float[] buyReturns) {

		ArrayList<Integer> buyDates = new ArrayList<Integer>();
		ArrayList<Integer> sellDates = new ArrayList<Integer>();
		ArrayList<Float> balances = new ArrayList<Float>();

		buyDates.add(lowerIndex);
		int buyIndex = lowerIndex;
		//		int sellIndex = -1;
		float balance = INITIAL_INVESTMENT;
		StateEnum state = StateEnum.looking_to_sell;

		for (int i = lowerIndex + 1; i <= upperIndex; i++) {
			switch (state) {
			case looking_to_sell:
				int index = Math.max(i, buyIndex);
				if (sellReturns[index] <= sellCriteria) {
					balance = calculateGainDollars(balance, buyIndex, index);					
					state = StateEnum.looking_to_buy;
					sellDates.add(index);
					balances.add(balance);
					//					sellIndex = index;
				}

				break;

			case looking_to_buy:
				boolean buy = false;
				if (buyReturns[i] >= buyCriteria)
					buy = true;
				//				else if (i - sellIndex >= 20) {
				//					float returnSinceSell = sandP[sellIndex].close / (sandP[i].close - sandP[sellIndex].close);
				//					if (returnSinceSell >= 0.2f)
				//						buy = true;
				//				}
				//				
				if (buy) {				
					buyDates.add(i);
					buyIndex = i;
					state = StateEnum.looking_to_sell;
				}
				break;
			}
		}

		if (state == StateEnum.looking_to_sell) {
			balance = calculateGainDollars(balance, buyIndex, upperIndex);
			sellDates.add(upperIndex);
			balances.add(balance);
		}

		CalculateGainResult cgr = new CalculateGainResult();
		cgr.balances = balances;
		cgr.buyDates = buyDates;
		cgr.sellDates = sellDates;
		cgr.finalBalance = balance;
		return cgr;

	}

	private static final long addMonths(long date, int months) {
		GregorianCalendar cal = new GregorianCalendar();
		cal.setTime(new Date(date));
		cal.add(GregorianCalendar.MONTH, months);
		return cal.getTimeInMillis();
	}

	private static void calc1YearReturns(long beginDate, long endDate, float sellCriteria, float buyCriteria, int sellDays, int buyDays) 
			throws InterruptedException {

		System.out.println("Bal if held, BFIM bal, Pct");
		for (long begin = beginDate; begin <= endDate; begin = addMonths(begin, 12)) {
			long end = addMonths(begin, 12);
			int[] dateRange = getDateIndexRange(begin, end);
			SandPAnalysis spa = new SandPAnalysis(dateRange[0], dateRange[1], sellCriteria, buyCriteria, sellDays, buyDays);
			spa.run();

			float baseBal = INITIAL_INVESTMENT + INITIAL_INVESTMENT * calculateGain(dateRange[0], dateRange[1]);
			float resultBal = results.peek().balance;
			float deltaPct = (resultBal - baseBal) / baseBal;
			results.clear();


			System.out.println(sdf.format(new Date(begin)) + "," + baseBal + "," + resultBal + ", " + deltaPct);
		}						
	}


	private static void calc20YearReturns(long beginDate, long endDate, float sellCriteria, float buyCriteria, int sellDays, int buyDays) 
			throws InterruptedException {

		System.out.println("Bal if held, BFIM bal, Pct");
		for (long begin = beginDate; begin <= endDate; begin = addMonths(begin, 12)) {
			long end = addMonths(begin, 20 * 12);
			int[] dateRange = getDateIndexRange(begin, end);
			SandPAnalysis spa = new SandPAnalysis(dateRange[0], dateRange[1], sellCriteria, buyCriteria, sellDays, buyDays);
			spa.run();

			float baseBal = INITIAL_INVESTMENT + INITIAL_INVESTMENT * calculateGain(dateRange[0], dateRange[1]);
			float resultBal = results.peek().balance;
			float deltaPct = (resultBal - baseBal) / baseBal;
			results.clear();


			System.out.println(sdf.format(new Date(begin)) + "," + baseBal + "," + resultBal + ", " + deltaPct);
		}						
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			//			System.setOut(new PrintStream("c:\\temp\\out"));
			loadData("data/sandp.csv");				

			//			for (int i = 0; i < sandP.length; i++) {
			//				System.out.println(i + " " + sandP[i]);
			//			}

			//			long beginDate = sdf.parse("1/1/1950").getTime();
			//			long endDate = sdf.parse("10/27/2008").getTime();
			//			int[] dateRange = getDateIndexRange(beginDate, endDate);										

			//			System.out.println(calculateGain(dateRange[0], dateRange[1], -0.075f, 0.7f));
			//			System.exit(0);

			float beginSellCriteria = -0.02f;
			float endSellCriteria = -0.15f;
			float sellCriteriaIncrement = 0.005f;

			float beginBuyCriteria = 0.02f;
			float endBuyCriteria = 0.15f;
			float buyCriteriaIncrement = 0.005f;

			ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2, 5000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(30));
			executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

			calculateReturnsSincePeriod(1, 30);

			long beginDate = sdf.parse("1/1/1950").getTime();
			long endDate = sdf.parse("5/1/2017").getTime();

			calc20YearReturns(beginDate, sdf.parse("5/1/1997").getTime(), -.075f, .035f, 18, 4);
//			calc1YearReturns(beginDate, sdf.parse("1/1/2016").getTime(), -.075f, .035f, 18, 4);
			System.exit(0);

			//			for (long beginDate = sdf.parse("1/1/1950").getTime(); beginDate <= sdf.parse("7/1/1988").getTime(); beginDate = addMonths(beginDate, 6)) {
			//				long endDate = addMonths(beginDate, 20 * 12);
			int[] dateRange = getDateIndexRange(beginDate, endDate);
			for (int sellDays = 2; sellDays <= 30; sellDays++) {
				for (int buyDays = 2; buyDays <= 30; buyDays++) {
					//						System.out.println(sellDays + " " + buyDays);
					for (float sell = beginSellCriteria; sell >= endSellCriteria; sell -= sellCriteriaIncrement) {
						for (float buy = beginBuyCriteria; buy <= endBuyCriteria; buy += buyCriteriaIncrement) {
							SandPAnalysis spa = new SandPAnalysis(dateRange[0], dateRange[1], sell, buy, sellDays, buyDays);
							executor.execute(spa);
							//							System.out.println("adding");
						}
					}	
				}
			}

			while (executor.getQueue().size() != 0 || results.size() == 0)
				Thread.sleep(1000);

			//				System.out.println("Results for the 20 year period beginning " + sdf.format(new Date(beginDate)));
			float baseBal = INITIAL_INVESTMENT + INITIAL_INVESTMENT * calculateGain(dateRange[0], dateRange[1]);
			float resultBal = results.peek().balance;

			System.out.println("Balance if bought and held " + baseBal);
			for (FinalResult fr: results) {
				System.out.println(fr);
			}
			results.clear();

			float deltaPct = (resultBal - baseBal) / baseBal;
			//				System.out.println("Delta: " + deltaPct);

			System.out.println(sdf.format(new Date(beginDate)) + ", " + deltaPct);

			//			}

			//			System.out.println("Finished adding");
			//			while (executor.getQueue().size() != 0)
			//				Thread.sleep(1000);
			//			
			//			float baseBal = INITIAL_INVESTMENT + INITIAL_INVESTMENT * calculateGain(dateRange[0], dateRange[1]);
			//			float resultBal = results.peek().balance;
			//			
			//			System.out.println(baseBal);
			//			for (FinalResult fr: results) {
			//				System.out.println(fr);
			//			}
			//			
			//			float deltaPct = (resultBal - baseBal) / baseBal;
			//			System.out.println(deltaPct);
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		System.exit(0);

	}

}

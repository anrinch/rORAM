package utils;

import java.util.Collections;
import java.util.List;


public class MathUtils {
private static final MathUtils instance = new MathUtils();
	
	private MathUtils() {}
	
	public static MathUtils getInstance() { return instance; }
	
	private double quantile(List<Double> list, double q)
	{
		int s = list.size();
		int i = (int)Math.floor(q * s);

		return list.get(i);
	}
	
	public class SummaryStats
	{
		public int n = 0;
		public double min = 0.0;
		public double max = 0.0;
		public double median = 0.0;
		public double q1 = 0.0;
		public double q3 = 0.0;
		public double iqr = 0.0;
		public double q95 = 0.0;
		public double q99 = 0.0;
		
		public double mean = 0.0;
		public double stdev = 0.0;
	}
	
	public SummaryStats summarize(List<Double> list)
	{
		Collections.sort(list);
		
		SummaryStats ret = new SummaryStats();
		ret.n = list.size();
		ret.min = list.get(0); 
		ret.max = list.get(list.size()-1);
		ret.median = quantile(list, 0.5);
		ret.q1 = quantile(list, 0.25);
		ret.q3 = quantile(list, 0.75);
		ret.iqr = ret.q3 - ret.q1;
		ret.q95 = quantile(list, 0.95);
		ret.q99 = quantile(list, 0.99);
		
		ret.mean = mean(list);
		ret.stdev = stdev(list, ret.mean);
		
		return ret;
	}

	private double stdev(List<Double> list, double mean) 
	{
		double sum = 0.0; for(double d : list) { sum += Math.pow((d - mean), 2); }
		return Math.sqrt(sum / list.size());
	}

	private double mean(List<Double> list) 
	{
		Errors.verify(list.isEmpty() == false);
		double sum = 0.0; for(double d : list) { sum += d; }
		return sum / list.size();
	}
}

package patterns.summarization.numerical.average;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple> {
	
	public void reduce(IntWritable key, Iterable<CountAverageTuple> values, Context context) throws IOException, InterruptedException {
		
		float sum = 0,  count = 0;
		
		// Iterate through all input values for this key
		for (CountAverageTuple val : values) {
			sum = sum + val.getCount() * val.getAverage();
			count = count + val.getCount();
		}
		
		// get the comment length
		CountAverageTuple result = new CountAverageTuple(count, sum/count);
		
		// write out the hour with the comment length
		context.write(key, result);
	}
}
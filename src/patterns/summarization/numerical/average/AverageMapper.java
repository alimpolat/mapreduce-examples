package patterns.summarization.numerical.average;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import patterns.util.MRDPUtils;

public class AverageMapper extends Mapper<Object, Text, IntWritable, CountAverageTuple> {
	
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
		
		// Get the "CreationDate" field, since it is what we are grouping by
		String strDate = parsed.get("CreationDate");

		// Get the comment to find the length
		String text = parsed.get("Text");
		
		if (strDate == null || text == null) {
			return;
		}
		
		// get the hour this comment was posted in
		Date creationDate;
		try {
			creationDate = frmt.parse(strDate);
		} catch (ParseException e) {
			return;
		}
		
		Calendar cal = Calendar.getInstance();
		cal.setTime(creationDate);
		
		IntWritable outHour = new IntWritable(cal.get(Calendar.HOUR_OF_DAY));
		
		// get the comment length
		CountAverageTuple outCountAverage = new CountAverageTuple(1, text.length());
		
		// write out the hour with the comment length
		context.write(outHour, outCountAverage);
	}
}

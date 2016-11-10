package patterns.filtering.topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import patterns.util.MRDPUtils;

/*
 * IMPORTANT: Here we write to the context in the cleanup method and not in the map method.
 * 
 * For every map task when the mapper is instantiated the setup and cleanup method are called once, 
 * whereas the map method will be called for every record.
 * 
 * Since we have to ensure that each individual map task emits a list of top ten records only after it 
 * has processed its entire set of allocated input split records so we need to write to the context in 
 * the cleanup method.
 * 
 */
public class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
	
	// Stores map od user reputation to the record
	private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

		String userId = parsed.get("Id");
		String reputation = parsed.get("Reputation");

		if (StringUtils.isNotEmpty(userId) && StringUtils.isNumeric(reputation)) {

			// Add this record to our map with the reputation as the key
			repToRecordMap.put(Integer.parseInt(reputation), new Text(userId));
			
			// If we have more than ten records, remove the one with the lowest reputation
			// As this is a TreeMap is sorted in ascending order, the user with the lowest reputation  is the first key
			if (repToRecordMap.size() > 10) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		// Output our ten records to the reducers with a null key
		for (Text t : repToRecordMap.values()) {
			context.write(NullWritable.get(), t);
		}
	}
}
package patterns.filtering.topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import patterns.util.MRDPUtils;

public class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
	
	// Stores a map of user repuation to the record
	// Overloads the comparator to order the reputations in descending order
	private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	@Override
	public void reduce(NullWritable key, Iterable<Text> values,	Context context) throws IOException, InterruptedException {
		
		for (Text value : values) {
			
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

			String userId = parsed.get("Id");
			String reputation = parsed.get("Reputation");

			// Add this record to our map with the reputation as the key
			repToRecordMap.put(Integer.parseInt(reputation), new Text(userId));
			
			// If we have more than ten records, remove the one with the lowest reputation
			// As this is a TreeMap is sorted in ascending order, the user with the lowest reputation  is the first key
			if (repToRecordMap.size() > 10) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}
		
		// Output our ten records to the file system with a null key
		for(Text value: repToRecordMap.descendingMap().values()) {
			context.write(NullWritable.get(), value);
		}
	}
}

package patterns.filtering.distinct;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import patterns.util.MRDPUtils;

public class DistinctUserMapper extends Mapper<Object, Text, Text, NullWritable> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

		// Get the value for the UserId attribute
		String userId = parsed.get("UserId");

		if (userId != null) {
			// Write the user's id with a null value
			context.write(new Text(userId), NullWritable.get());
		}
	}
}

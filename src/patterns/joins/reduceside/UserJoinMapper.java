package patterns.joins.reduceside;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import patterns.util.MRDPUtils;

public class UserJoinMapper extends Mapper<Object, Text, Text, Text> {
	
	private Text outkey = new Text();
	private Text outvalue = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		// Parse the input string into a nice map
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
		
		String userId = parsed.get("Id");
		if (userId == null) {
			return;
		}
		
		// The foreign join key is the user ID
		outkey.set(userId);
		
		// Flag this record for the reducer and then output
		outvalue.set("A" + value.toString());
		
		context.write(outkey, outvalue);
	}
}
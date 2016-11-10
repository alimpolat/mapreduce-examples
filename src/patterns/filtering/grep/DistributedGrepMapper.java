package patterns.filtering.grep;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributedGrepMapper extends Mapper<Object, Text, NullWritable, Text> {

	public static final String REGEX_KEY = "patterns.filtering.grep.regex";
	private Pattern pattern = null;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		pattern = Pattern.compile(context.getConfiguration().get(REGEX_KEY));
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Matcher matcher = pattern.matcher(value.toString());
		if (matcher.find()) {
			context.write(NullWritable.get(), value);
		}
	}
}
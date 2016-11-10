package patterns.summarization.numerical.minmaxcount;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import patterns.summarization.numerical.average.AverageDriver;
import patterns.summarization.numerical.average.AverageMapper;
import patterns.summarization.numerical.average.AverageReducer;
import patterns.summarization.numerical.average.CountAverageTuple;

public class MinMaxCountDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <comments_in> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}

		Job job = Job.getInstance(getConf(), "StackOverflow Comment MinMax Count");
		job.setJarByClass(getClass());

		job.setMapperClass(MinMaxCountMapper.class);
		job.setReducerClass(MinMaxCountReducer.class);
		job.setCombinerClass(MinMaxCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MinMaxCountTuple.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MinMaxCountDriver(), args);
		System.exit(res);
	}

}

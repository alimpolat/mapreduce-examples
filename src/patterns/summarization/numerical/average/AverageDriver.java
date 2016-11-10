package patterns.summarization.numerical.average;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageDriver extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
				
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <comments_in> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
				
		Job job = Job.getInstance(getConf(), "StackOverflow Comment Average Length");
		job.setJarByClass(getClass());
		
		job.setMapperClass(AverageMapper.class);
		job.setReducerClass(AverageReducer.class);
		job.setCombinerClass(AverageReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(CountAverageTuple.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		FileSystem.get(getConf()).delete(outputDir, true);
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AverageDriver(), args);
		System.exit(res);
	}
}

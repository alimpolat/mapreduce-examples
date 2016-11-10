package patterns.filtering.grep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * IMPORTANT: Remember to configure the job with 0 reducer!
 */

public class DistributedGrepDriver extends Configured implements Tool {
	
	
	@Override
	public int run(String[] args) throws Exception {
				
		if (args.length != 3) {
			System.err.printf("Usage: %s [generic options] <regex> <comments_in> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
				
		Job job = Job.getInstance(getConf(), "StackOverflow Distributed Grep");
		job.setJarByClass(getClass());
		
		job.setMapperClass(DistributedGrepMapper.class);
		
		// IMPORTANT: Set number of Reducers to 0
		job.setNumReduceTasks(0);
		job.getConfiguration().set(DistributedGrepMapper.REGEX_KEY, args[0]);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		FileSystem.get(getConf()).delete(outputDir, true);
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DistributedGrepDriver(), args);
		System.exit(res);
	}	
}

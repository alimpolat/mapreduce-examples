package patterns.filtering.topten;

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
 * IMPORTANT: Remember to configure the job to use only one reducer! Multiple reducers would shard the data
 * and would result in multiple "top ten" lists.
 */
public class TopTenUsersByReputationDriver extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
				
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <users_in> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
				
		Job job = Job.getInstance(getConf(), "StackOverflow Top Ten Users By Reputation");
		job.setJarByClass(getClass());
		
		job.setMapperClass(TopTenMapper.class);
		job.setReducerClass(TopTenReducer.class);
		
		// IMPORTANT:
		job.setNumReduceTasks(1);
		
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
		int res = ToolRunner.run(new Configuration(), new TopTenUsersByReputationDriver(), args);
		System.exit(res);
	}	
}

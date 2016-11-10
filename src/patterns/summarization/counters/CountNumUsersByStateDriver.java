package patterns.summarization.counters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * IMPORTANT: Remember to configure the job to use only one reducer! Multiple reducers would shard the data
 * and would result in multiple "top ten" lists.
 */
public class CountNumUsersByStateDriver extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
				
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <users_in> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
				
		Job job = Job.getInstance(getConf(), "StackOverflow Number of Users by State");
		job.setJarByClass(getClass());
		
		job.setMapperClass(CountNumUsersByStateMapper.class);
		// IMPORTANT: We do not need any Redcuer here
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		FileSystem.get(getConf()).delete(outputDir, true);
		boolean success = job.waitForCompletion(true);

		if (success) {
			for (Counter counter : job.getCounters().getGroup(
					CountNumUsersByStateMapper.STATE_COUNTER_GROUP)) {
				System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
			}
		}
		
		// IMPORTANT: Remember to delete the Output directory, otherwise it will have the blank SUCCESS file
		FileSystem.get(getConf()).delete(outputDir, true);
		
		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CountNumUsersByStateDriver(), args);
		System.exit(res);
	}	
}

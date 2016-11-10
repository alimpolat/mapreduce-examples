package patterns.filtering.distinct;

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

public class DistinctUserDriver extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
				
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <comments_in> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
				
		Job job = Job.getInstance(getConf(), "StackOverflow Distinct User");
		job.setJarByClass(getClass());
		
		job.setMapperClass(DistinctUserMapper.class);
		job.setReducerClass(DistinctUserReducer.class);
		job.setCombinerClass(DistinctUserReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		FileSystem.get(getConf()).delete(outputDir, true);
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DistinctUserDriver(), args);
		System.exit(res);
	}	
}

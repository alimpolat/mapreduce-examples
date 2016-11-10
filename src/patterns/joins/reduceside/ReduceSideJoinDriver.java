package patterns.joins.reduceside;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceSideJoinDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 4) {
			System.err.printf("Usage: %s [generic options] <user_in> <comments_in> <join_type> <out>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}

		Job job = Job.getInstance(getConf(), "StackOverflow ReduceSideJoin User & Comments");
		job.setJarByClass(getClass());

		// Use MultipleInputs to set which input uses what mapper
		// This will keep parsing of each data set separate from a logical standpoint
		// The first two elements of the args array are the two inputs
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserJoinMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CommentJoinMapper.class);
		job.getConfiguration().set("join.type", args[2]);

		job.setReducerClass(UserJoinReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path outputDir = new Path(args[3]);
		TextOutputFormat.setOutputPath(job, outputDir);

		FileSystem.get(getConf()).delete(outputDir, true);
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ReduceSideJoinDriver(), args);
		System.exit(res);
	}
}

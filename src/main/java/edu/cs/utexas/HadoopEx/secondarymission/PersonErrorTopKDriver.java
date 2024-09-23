package edu.cs.utexas.HadoopEx.secondarymission;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PersonErrorTopKDriver extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PersonErrorTopKDriver(), args);
		System.exit(res);
	}

	/**
	 * 
	 */
	public int run(String args[]) {
		try {
			Configuration conf = new Configuration();

			Job job = new Job(conf, "Person Error Job guy");
			job.setJarByClass(PersonErrorTopKDriver.class);

			// specify a Mapper
			job.setMapperClass(PersonErrorJobMapper.class);

			// specify a Reducer
			job.setReducerClass(PersonErrorJobReducer.class);

			// specify output types
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			// specify input and output directories
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setOutputFormatClass(TextOutputFormat.class);

			if (!job.waitForCompletion(true)) {
				return 1;
			}

			Job job2 = new Job(conf, "Top 5 Job Guy");
			job2.setJarByClass(PersonErrorTopKDriver.class);

			// specify a Mapper
			job2.setMapperClass(TopKJobMapper.class);

			// specify a Reducer
			job2.setReducerClass(TopKJobReducer.class);

			// specify output types
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(FloatWritable.class);

			// set the number of reducer to 1
			job2.setNumReduceTasks(1);

			// specify input and output directories
			FileInputFormat.addInputPath(job2, new Path(args[1]));
			job2.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			job2.setOutputFormatClass(TextOutputFormat.class);

			return (job2.waitForCompletion(true) ? 0 : 1);

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during driver job.");
			e.printStackTrace();
			return 2;
		}
	}
}

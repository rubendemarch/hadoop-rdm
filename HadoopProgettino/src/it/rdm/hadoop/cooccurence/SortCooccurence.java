package it.rdm.hadoop.cooccurence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * 
 * @author rdemarch
 * 
 *         Driver class for sorting the Co-occurrence output
 * 
 */
public class SortCooccurence extends Configured implements Tool {

	private int numberOfReducers;
	private Path inputPath;
	private Path outputDir;

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = this.getConf();
		// define new job
		Job job = new Job(conf);

		// assign a name to the job
		job.setJobName("Ruben De March's Sorting Co-Occurence");

		// set input path for the job
		FileInputFormat.addInputPath(job, inputPath);

		// set output path for the job
		FileOutputFormat.setOutputPath(job, outputDir);

		// set the jar class
		job.setJarByClass(getClass());

		// set job input format
		job.setInputFormatClass(TextInputFormat.class);

		// set map class
		job.setMapperClass(SortingMapper.class);

		// set map output key and value classes
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//set map output comparator, decreasing order
		job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

		// set reduce class
		job.setReducerClass(SortingReducer.class);

		// set number of reducers
		job.setNumReduceTasks(numberOfReducers);

		// set reduce output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// set job output format
		job.setOutputFormatClass(TextOutputFormat.class);

		// execute the job and wait for completion
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public SortCooccurence(int numberOfReducers, Path inputPath, Path outputPath) {
		this.numberOfReducers = numberOfReducers;
		this.inputPath = inputPath;
		this.outputDir = outputPath;
	}

}

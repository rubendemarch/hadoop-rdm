package it.rdm.hadoop.cooccurence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Co-occorrenze
 * Input: un file testuale
 * Output: frequenza delle coppie di parole che co­occorrono su una stessa riga
 * 
 *  
 *  Pseudocode:
 * 
 * mapper(key, value){
 * foreach riga in value
 * 	set list(distinteParoleinRiga) = {parole distinte in riga}
 * 	foreach (coppia di parole) in distinteParoleinRiga:
 * 		emit(coppia,1)
 * }
 * 
 * reducer(key, list(values)){
 * 	set count = 0
 * 	foreach value in values:
 * 		count += value
 * 	emit(key, count)
 * }
 *
 *
 * Driver class.
 */
public class Cooccurence extends Configured implements Tool {

  private int numberOfReducers;
  private Path inputPath;
  private Path outputDir;

  @Override
  public int run(String[] args) throws Exception {

    Configuration conf = this.getConf();
    //  define new job
    Job job = new Job(conf);

    //  assign a name to the job
    job.setJobName("Ruben De March's Co-Occurence");
    
    //  set input path for the job
    FileInputFormat.addInputPath(job, inputPath);
    
    //  set output path for the job
    FileOutputFormat.setOutputPath(job, outputDir);
    
    // TODO set the jar class
    job.setJarByClass(getClass());
    
    //  set job input format
    job.setInputFormatClass(TextInputFormat.class);
    
    //  set map class
    job.setMapperClass(CooccurenceMapper.class);
    
    //  set map output key and value classes
    job.setMapOutputKeyClass(TextCouple.class);
    job.setMapOutputValueClass(LongWritable.class);
    
    //  set reduce class
    job.setReducerClass(CooccurenceReducer.class);
    
    //  set number of reducers
    job.setNumReduceTasks(numberOfReducers);
    
    //  set reduce output key and value classes
    job.setOutputKeyClass(TextCouple.class);
    job.setOutputValueClass(LongWritable.class);
    
    //  set job output format
    job.setOutputFormatClass(TextOutputFormat.class);
    
    //  execute the job and wait for completion
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public Cooccurence (String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: WordCount <num_reducers> <input_path> <output_path>");
      System.exit(0);
    }
    
    this.numberOfReducers = Integer.parseInt(args[0]);
    this.inputPath = new Path(args[1]);
    this.outputDir = new Path(args[2]);
  }
  
  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Cooccurence(args), args);
    int res2 = ToolRunner.run(new Configuration(), new SortCooccurence(args), args);
    System.exit(res2);
  }
}
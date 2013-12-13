package it.rdm.hadoop.invertIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Indice invertito
 * Input: una directory contenente un insieme di file testuali
 * Output: un indice che indichi per ciascuna parola, presente nei file di input, il
 * nome dei file che contengono tale parola
 *
 *
 * Pseudocode:
 * 
 * mapper(key, value){
 * 	foreach word in value:
 * 		emit(word, fileSplit name)
 * }
 * 
 * reducer(key, list(values)){
 * 	set outputString = ""
 * 	foreach value in values:
 * 		if value not in outputString
 * 			append value to outputstring
 * 	emit(key,outputString)
 * }
 *
 *
 * Driver class.
 */
public class InvertIndex extends Configured implements Tool {

  private int numberOfReducers;
  private Path inputPath;
  private Path outputDir;

  @Override
  public int run(String[] args) throws Exception {

    Configuration conf = this.getConf();
    //  define new job
    Job job = new Job(conf);

    //  assign a name to the job
    job.setJobName("Ruben De March's Invert Index");
    
    //  set input path for the job
    FileInputFormat.addInputPath(job, inputPath);
    
    //  set output path for the job
    FileOutputFormat.setOutputPath(job, outputDir);
    
    // set the jar class
    job.setJarByClass(getClass());
    
    //  set job input format
    job.setInputFormatClass(TextInputFormat.class);
    
    //  set map class
    job.setMapperClass(InvertIndexMapper.class);
    
    //  set map output key and value classes
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    //  set reduce class
    job.setReducerClass(InvertIndexReducer.class);
    
    //  set number of reducers
    job.setNumReduceTasks(numberOfReducers);
    
    //  set reduce output key and value classes
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    //  set job output format
    job.setOutputFormatClass(TextOutputFormat.class);
    
    //  execute the job and wait for completion
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public InvertIndex (String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: InvertIndex <num_reducers> <input_path> <output_path>");
      System.exit(0);
    }
    
    this.numberOfReducers = Integer.parseInt(args[0]);
    this.inputPath = new Path(args[1]);
    this.outputDir = new Path(args[2]);
  }
  
  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new Configuration(), new InvertIndex(args), args);
    System.exit(res);
  }
}
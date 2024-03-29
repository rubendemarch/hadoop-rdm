package it.polito.dbdmg.hadoop.wordcount;

/**
 * @author Luigi Grimaudo
 * @email luigi.grimaudo@polito.it
 * @version 0.0.1
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * WordCount Mapper
 */
class WordCountMapper extends Mapper<
                    LongWritable, // input key type passed by TextInputFormat
                    Text,         //  input value type
                    Text,         // replace Object with output key type
                    LongWritable> {//  replace Object with output value type
    
    @Override
    protected void map(
    		LongWritable key,   //  input key type
            Text value,         //  input value type
            Context context) throws IOException, InterruptedException {

            String[] words = value.toString().split("\\s+");
            
            for (String string : words) {
            	context.write(new Text(string.toLowerCase()), new LongWritable(1));
			}
    }
}
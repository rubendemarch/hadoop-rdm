package it.rdm.hadoop.cooccurence;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author rdemarch
 *
 */
public class SortingMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
    @Override
    protected void map(
    		LongWritable key,   //  input key type
            Text value,         //  input value type
            Context context) throws IOException, InterruptedException {
    	// Creates an array with the couple of words and its frequency 
    	// (they are separated with the tab character in the previous output)
    	String[] coppia = value.toString().split("\\t+");
    	// The emitted key is the frequency, so that Shuffle&Sort sorts by frequency
    	context.write(new LongWritable(Long.parseLong(coppia[1])), new Text(coppia[0]));
    }
    

}

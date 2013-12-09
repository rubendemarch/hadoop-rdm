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
public class SortingMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
    @Override
    protected void map(
    		LongWritable key,   //  input key type
            Text value,         //  input value type
            Context context) throws IOException, InterruptedException {
    	context.write(value, key);
    }
    

}

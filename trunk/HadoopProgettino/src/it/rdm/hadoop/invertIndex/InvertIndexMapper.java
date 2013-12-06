package it.rdm.hadoop.invertIndex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * InvertIndex Mapper
 */
class InvertIndexMapper extends Mapper<
LongWritable, // input key type passed by TextInputFormat
                    Text,         //  input value type
                    Text,         // replace Object with output key type
                    Text> {//  replace Object with output value type
    
    @Override
    protected void map(
    		LongWritable key,   //  input key type
            Text value,         //  input value type
            Context context) throws IOException, InterruptedException {

            String[] words = value.toString().split("\\s+");
            
            for (String string : words) {
            	String cleanWord = string.replaceAll("[^\\p{Alpha}]", "");
            	if (!cleanWord.isEmpty()) {
					context.write(new Text(cleanWord.toLowerCase()),new Text(((FileSplit)context.getInputSplit()).getPath().getName()));
				}
			}
    }
}
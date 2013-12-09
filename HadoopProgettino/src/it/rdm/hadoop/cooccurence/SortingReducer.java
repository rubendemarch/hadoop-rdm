/**
 * 
 */
package it.rdm.hadoop.cooccurence;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author rdemarch
 *
 */
public class SortingReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

	@Override
    protected void reduce(
    	LongWritable key, // input key type
        Iterable<Text> values, // input value type
        Context context) throws IOException, InterruptedException {

        for (Text coppiaDiParole : values) {
        	context.write(coppiaDiParole, key);
		}
    }
}

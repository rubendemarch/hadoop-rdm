package it.polito.dbdmg.hadoop.wordcount;

/**
 * @author Luigi Grimaudo
 * @email luigi.grimaudo@polito.it
 * @version 0.0.1
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount Reducer
 */
class WordCountReducer extends Reducer<
                Text,           //   input key type
                LongWritable,    //   input value type
                Text,           //   output key type
                LongWritable> {  //   output value type
    
    @Override
    protected void reduce(
        Text key, // input key type
        Iterable<LongWritable> values, // input value type
        Context context) throws IOException, InterruptedException {

        int count = 0;
        for (LongWritable intWritable : values) {
			count += intWritable.get();
		}
        
        context.write(key, new LongWritable(count));
    }
}

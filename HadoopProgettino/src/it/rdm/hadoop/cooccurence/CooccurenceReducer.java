package it.rdm.hadoop.cooccurence;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Co-occurence Reducer
 */
class CooccurenceReducer extends Reducer<
                TextCouple,           //   input key type
                LongWritable,    //   input value type
                TextCouple,           //   output key type
                LongWritable> {  //   output value type
    
    /**
     * It simply emits the key along with the sum of all values in the list.
     */
	@Override
    protected void reduce(
        TextCouple key, // input key type
        Iterable<LongWritable> values, // input value type
        Context context) throws IOException, InterruptedException {

        int count = 0;
        for (LongWritable intWritable : values) {
			count += intWritable.get();
		}
        
        context.write(key, new LongWritable(count));
    }
}

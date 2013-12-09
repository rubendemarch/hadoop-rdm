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
    	String[] coppia = value.toString().split("\\t+");//per ogni riga di output del reducer separa la coppia di parole e la frequenza
    	context.write(new LongWritable(Long.parseLong(coppia[1])), new Text(coppia[0]));
    }
    

}

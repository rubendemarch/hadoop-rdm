package it.rdm.hadoop.cooccurence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Co-occurence Mapper
 */
class CooccurenceMapper extends Mapper<
                    LongWritable, // input key type passed by TextInputFormat
                    Text,         //  input value type
                    TextCouple,         // replace Object with output key type
                    LongWritable> {//  replace Object with output value type
    
    @Override
    protected void map(
    		LongWritable key,   //  input key type
            Text value,         //  input value type
            Context context) throws IOException, InterruptedException {

//            String[] rows = value.toString().split("\\n+");//separo attraverso gli a-capo, non serve perché fatto in automatico
            
//            for (String row : rows) {
				List<String> cleanRow = new ArrayList<String>();
				
				String[] words = value.toString().split("\\s+");
	            for (String string : words) {
	            	String cleanWord = string.replaceAll("[^\\p{Alpha}]", "").toLowerCase();
	            	if (!cleanWord.isEmpty() & !cleanRow.contains(cleanWord)){
	            		cleanRow.add(cleanWord);
	            	}
	            }
	            for (int i = 0; i < cleanRow.size(); i++) {
	            	for (int j = 0; j < i; j++) {
	            		context.write(new TextCouple(cleanRow.get(i), cleanRow.get(j)), new LongWritable(1));
					}
				}
//            }
    }
}
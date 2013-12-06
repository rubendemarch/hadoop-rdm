package it.rdm.hadoop.invertIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * InvertIndex Reducer
 */
class InvertIndexReducer extends Reducer<
                Text,           //   input key type
                Text,    //   input value type
                Text,           //   output key type
                Text> {  //   output value type
    
    @Override
    protected void reduce(
        Text key, // input key type
        Iterable<Text> values, // input value type
        Context context) throws IOException, InterruptedException {

        StringBuilder output = new StringBuilder();
        List<String> cleanValues = new ArrayList<String>();
        for (Text fileName : values) {
    		String string = fileName.toString();
        	if (!cleanValues.contains(string)){
				output.append(string + " ");
				cleanValues.add(string);
        	}
		}
        output.deleteCharAt(output.length()-1);
        context.write(key, new Text(output.toString()));
    }
}

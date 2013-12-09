/**
 * 
 */
package it.rdm.hadoop.cooccurence;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author rdemarch
 *
 */
public class SortingReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

}

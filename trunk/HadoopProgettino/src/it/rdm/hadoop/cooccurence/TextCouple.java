package it.rdm.hadoop.cooccurence;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextCouple implements WritableComparable<TextCouple> {

	Text word1; //= new Text();
	Text word2; //= new Text();
	
	@Override
	public void readFields(DataInput in) throws IOException {
		word1.readFields(in);
		word2.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		word1.write(out);
		word2.write(out);
	}

	@Override
	public int compareTo(TextCouple o) {
		int b;
		if((b = this.word1.toString().compareTo(o.word1.toString()))!=0){
			return b;
		}
		if((b = this.word2.toString().compareTo(o.word2.toString()))!=0){
			return b;
		}
		return 0;
	}
	

	
	@Override
	public String toString() {
		return word1 + " - " + word2;
	}
	
	public TextCouple(){
		this.word1 = new Text();
		this.word2 = new Text();
	}
	
	TextCouple(String string1, String string2){
		if (string1.compareTo(string2)<0){
			this.word1 = new Text(string1);
			this.word2 = new Text(string2);
		} else {
			this.word1 = new Text(string2);
			this.word2 = new Text(string1);
		}
	}
}

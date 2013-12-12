package it.rdm.hadoop.cooccurence;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextCouple implements WritableComparable<TextCouple> {

	// The two words in the couple
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

	/**
	 * Compare two {@link TextCouple} objects considering lexicographical order
	 */
	@Override
	public int compareTo(TextCouple o) {
		int b;
		// Returns the comparison between the two first words, if they are different
		if((b = this.word1.toString().compareTo(o.word1.toString()))!=0){
			return b;
		}
		// Returns the comparison between the two second words, if the two first ones are equal.
		return this.word2.toString().compareTo(o.word2.toString());
	}
	

	
	@Override
	public String toString() {
		//Custom way to display the TextCouple
		return word1 + " - " + word2;
	}
	
	public TextCouple(){
		this.word1 = new Text();
		this.word2 = new Text();
	}
	
	/**
	 * Constructs the {@link TextCouple} so that word1 <= word2 in lexicographic order 
	 * 
	 * @param string1
	 * @param string2
	 */
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

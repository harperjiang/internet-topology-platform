package edu.clarkson.cs.itop.tool.types;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntArrayWritable extends ArrayWritable {

	public IntArrayWritable() {
		super(IntWritable.class);
	}
	
	public IntArrayWritable(String[] data) {
		this();
		Writable[] ints = new Writable[data.length];
		for(int i = 0 ; i < ints.length;i++)
			ints[i] = new IntWritable(Integer.valueOf(data[i]));
		set(ints);
	}
}

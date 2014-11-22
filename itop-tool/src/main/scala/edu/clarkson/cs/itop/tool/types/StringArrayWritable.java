package edu.clarkson.cs.itop.tool.types;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StringArrayWritable extends ArrayWritable {

	public StringArrayWritable() {
		super(Text.class);
	}

	public StringArrayWritable(String[] data) {
		this();
		Writable[] strs = new Writable[data.length];
		for (int i = 0; i < strs.length; i++)
			strs[i] = new Text(data[i]);
		set(strs);
	}
}

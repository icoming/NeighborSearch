package zone.newapi.stat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class LongArrayWritable implements Writable, Cloneable {
	long array[];

	public LongArrayWritable() {
	}
	
	public LongArrayWritable(int len) {
		array = new long[len];
		for (int i = 0; i < len; i++)
			array[i] = 0;
	}
	
	public void set(int idx, long value) {
		array[idx] = value;
	}
	
	public void inc(int idx) {
		array[idx]++;
	}
	
	public void add(LongArrayWritable arr) {
		if (array.length != arr.array.length)
			throw new IllegalArgumentException();
		for (int i = 0; i < array.length; i++) {
			array[i] += arr.array[i];
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int len = in.readInt();
		array = new long[len];
		for (int i = 0; i < len; i++) {
			array[i] = in.readLong();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(array.length);
		for (int i = 0; i < array.length; i++) {
			out.writeLong(array[i]);
		}
	}

	@Override
	public String toString() {
		String ret = "";
		for (int i = 0; i < array.length; i++) {
			ret += array[i] + " ";
		}
		return ret;
	}

	public LongArrayWritable clone() {
		try {
			return (LongArrayWritable) super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
			return null;
		}
	}

}

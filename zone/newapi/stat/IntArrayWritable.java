package zone.newapi.stat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class IntArrayWritable implements Writable {
	int array[];
	
	public IntArrayWritable(int len) {
		array = new int[len];
		for (int i = 0; i < len; i++)
			array[i] = 0;
	}
	
	public IntArrayWritable(int array[]) {
		this.array = array;
	}
	
	public void set(int idx, int value) {
		array[idx] = value;
	}
	
	public void inc(int idx) {
		array[idx]++;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int len = in.readInt();
		array = new int[len];
		for (int i = 0; i < len; i++) {
			array[i] = in.readInt();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(array.length);
		for (int i = 0; i < array.length; i++) {
			out.write(array[i]);
		}
	}

}

package zone;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PairWritable implements Writable {
	private boolean isPair;
	private Star[] stars = new Star[2];
	
	public PairWritable () {
		isPair = false;
	}
	
	public PairWritable (Star star1, Star star2) {
		int num = 0;
		
		if (star1 != null) {
			stars[num] = star1;
			num++;
		}
		
		if (star2 != null) {
			stars[num] = star2;
			num++;
		}
		isPair = (num == 2);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		isPair = in.readBoolean();
		stars[0] = Star.createStar();
		stars[0].readFields(in);
		if (isPair) {
			stars[1] = Star.createStar();
			stars[1].readFields(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isPair);
		stars[0].write(out);
		if (isPair)
			stars[1].write(out);
	}

	public Star get(int index) {
		return stars[index];
	}
	
	@Override
	public String toString () {
		return "(" + stars[0] + "," + stars[1] + ")";
	}
}

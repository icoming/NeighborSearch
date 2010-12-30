package zone;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/* a Star object has 56 bytes.
 * but in an exported file, each object stored in the file has 1 more byte at the end.
 * therefore, each object actually occupies 57 bytes. */
public abstract class Star implements Writable, Cloneable {
	public double ra;			/* 8 bytes */
	public double dec;			/* 8 bytes */
	public long objID;			/* 8 bytes */
	public double x;			/* 8 bytes */
	public double y;			/* 8 bytes */
	public double z;			/* 8 bytes */
	public boolean margin;		/* 2 bytes */
	
	public String toString() {
		return new Long(objID).toString();// + "[" + ra + ":" + dec + "]";
	}
	
	public void print() {
		System.out.println("star " + objID + " ra: " + ra + ", dec: " + dec + ", margin: " + margin);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		ra = in.readDouble();
		dec = in.readDouble();
		objID = in.readLong();
		x = in.readDouble();
		y = in.readDouble();
		z = in.readDouble();
		margin = in.readBoolean();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(ra);
		out.writeDouble(dec);
		out.writeLong(objID);
		out.writeDouble(x);
		out.writeDouble(y);
		out.writeDouble(z);
		out.writeBoolean(margin);
	}
	
	public Star clone() {
		try {
			return (Star) super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
			return null;
		}
	}

	public abstract void set(DataInputStream in) throws IOException;
	
	public static Star createStar() {
		return new StarZone();
	}
	
	/**
	 * the size of the space occupied by a star in the input dataset.
	 */
	public abstract int size();
}

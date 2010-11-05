package zone;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/* a Star object has 56 bytes.
 * but in an exported file, each object stored in the file has 1 more byte at the end.
 * therefore, each object actually occupies 57 bytes. */
public class Star implements Writable {
	public static final int storeSize = 59;
	public int zoneNum;			/* 4 bytes */
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
		System.out.println("star " + objID + " ra: " + ra + ", dec: " + dec + ", zone: " + zoneNum + "margin: " + margin);
	}
	
	public Star () {
		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		zoneNum = in.readInt();
		ra = in.readDouble();
		dec = in.readDouble();
		objID = in.readLong();
		x = in.readDouble();
		y = in.readDouble();
		z = in.readDouble();
		margin = (in.readByte() != 1);
		
		in.readByte();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(zoneNum);
		out.writeDouble(ra);
		out.writeDouble(dec);
		out.writeLong(objID);
		out.writeDouble(x);
		out.writeDouble(y);
		out.writeDouble(z);
		out.writeByte(margin ? 0 : 1);
		
		out.writeByte(1);
	}

	public void set(DataInputStream in) throws IOException {
		zoneNum = Integer.reverseBytes(in.readInt());
		ra = Double.longBitsToDouble(Long.reverseBytes(in.readLong()));
		in.readByte();
		dec = Double.longBitsToDouble(Long.reverseBytes(in.readLong()));
		in.readByte();
		x = Double.longBitsToDouble(Long.reverseBytes(in.readLong()));
		in.readByte();
		y = Double.longBitsToDouble(Long.reverseBytes(in.readLong()));
		in.readByte();
		z = Double.longBitsToDouble(Long.reverseBytes(in.readLong()));
		objID = Long.reverseBytes(in.readLong());
		in.readByte();
		margin = Short.reverseBytes(in.readShort()) == 1;
	}
}

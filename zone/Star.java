package zone;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.Writable;

import zone.util.Converter;

/* a Star object has 56 bytes.
 * but in an exported file, each object stored in the file has 1 more byte at the end.
 * therefore, each object actually occupies 57 bytes. */
public class Star implements Writable {
	public static final int storeSize = 57;
	public int zoneNum;			/* 4 bytes */
	public double ra;			/* 8 bytes */
	public double dec;			/* 8 bytes */
	public long objID;			/* 8 bytes */
	public short type;			/* 2 bytes */
	public byte mode;			/* 1 bytes */
	public double x;			/* 8 bytes */
	public double y;			/* 8 bytes */
	public double z;			/* 8 bytes */
	public boolean margin;		/* 1 bytes */
	
	public String toString() {
		return new Long(objID).toString();// + "[" + ra + ":" + dec + "]";
	}
	
	public Star () {
		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		zoneNum = in.readInt();
		ra = in.readDouble();
		dec = in.readDouble();
		objID = in.readLong();
		type = in.readShort();
		mode = in.readByte();
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
		out.writeShort(type);
		out.writeByte(mode);
		out.writeDouble(x);
		out.writeDouble(y);
		out.writeDouble(z);
		out.writeByte(margin ? 0 : 1);
		
		out.writeByte(1);
	}

	public void set(byte[] bytes) {
		int offset = 0;
		zoneNum = Converter.array2Int(bytes, offset);
		offset += Integer.SIZE >> 3;
		ra = Converter.array2Double(bytes, offset);
		offset += Double.SIZE >> 3;
		dec = Converter.array2Double(bytes, offset);
		offset += Double.SIZE >> 3;
		objID = Converter.array2Long(bytes, offset);
		offset += Long.SIZE >> 3;
		type = Converter.array2Short(bytes, offset);
		offset += Short.SIZE >> 3;
		mode = bytes[offset];
		offset += 1;
		x = Converter.array2Double(bytes, offset);
		offset += Double.SIZE >> 3;
		y = Converter.array2Double(bytes, offset);
		offset += Double.SIZE >> 3;
		z = Converter.array2Double(bytes, offset);
		offset += Double.SIZE >> 3;
		margin = bytes[offset] != 1;
	}
}

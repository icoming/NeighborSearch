package zone;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * stars stored in the Zone table 
 */
public class StarZone extends Star {
	public static final int storeSize = 57;
	
	@Override
	public void set(DataInputStream in) throws IOException {
		in.readInt();		// zoneNum
		ra = Double.longBitsToDouble(Long.reverseBytes(in.readLong()));
		dec = Double.longBitsToDouble(Long.reverseBytes(in.readLong()));
		objID = Long.reverseBytes(in.readLong());
		in.readShort();		// type
		in.readByte();		// mode
		x = Double.longBitsToDouble(Long.reverseBytes(in.readLong()));
		y = Double.longBitsToDouble(Long.reverseBytes(in.readLong()));
		z = Double.longBitsToDouble(Long.reverseBytes(in.readLong()));
		margin = in.readByte() != 1;
		in.readByte();
	}

	@Override
	public int size() {
		return storeSize;
	}
}

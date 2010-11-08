package zone;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class BlockIDWritable implements WritableComparable<BlockIDWritable> {
	public int zoneNum;
	public int raNum;
	
	public BlockIDWritable() {
		
	}

	static public int ra2Num (double ra) {
		int raNum = (int) (ra / NeighborSearch.blockWidth);
		if (raNum == NeighborSearch.numBlocks) {
			raNum--;
		}
		return raNum;
	}
	
	public BlockIDWritable(double ra, double dec) {
		zoneNum = (int) ((dec + 90) / NeighborSearch.zoneHeight);
		if (zoneNum == NeighborSearch.numZones)
			zoneNum--;
		raNum = ra2Num (ra);
		/* zones in the poles have only one block. */
		if (zoneNum == 0 || zoneNum == NeighborSearch.numZones - 1)
			raNum = 0;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		zoneNum = in.readInt();
		raNum = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(zoneNum);
		out.writeInt(raNum);
	}

	@Override
	public int compareTo(BlockIDWritable o) {
		if (this.zoneNum != o.zoneNum)
			return this.zoneNum - o.zoneNum;
		return this.raNum - o.raNum;
	}
	
	@Override
	public int hashCode() {
		return (raNum) * 3;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof BlockIDWritable) {
			BlockIDWritable id = (BlockIDWritable) o;
			return id.zoneNum == this.zoneNum && id.raNum == this.raNum;
		}
		return false;
	}

	@Override
	public String toString() {
		return "(" + zoneNum + "," + raNum + ")";
	}
}

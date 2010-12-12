package zone.newapi;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import zone.BlockIDWritable;
import zone.Star;
import zone.PairWritable;

public class MapClass extends Mapper<LongWritable, Star, BlockIDWritable, PairWritable> {
	/* it seems it's very costly to create an object in Java.
	 * reuse these objects in every map invocation. */
	private BlockIDWritable loc = new BlockIDWritable();
	BlockIDWritable loc1 = new BlockIDWritable();
	PairWritable p = new PairWritable();
	
	public MapClass() {
		NeighborSearch.init();
	}

	public void map(LongWritable key, Star value, Context context)
			throws IOException, InterruptedException {
		loc.set(value.ra, value.dec);

		int zoneNum = loc.zoneNum;
		int raNum = loc.raNum;
		p.set (value, null);
		
		/*
		 * When the block size increases (> theta), only part of a block
		 * needs to be copied to its neighbor.
		 */
		context.write(loc, p);

		/*
		 * only replicate objects in the border of a block. I expect most of
		 * objects don't need to be copied.
		 */
		if (value.dec > NeighborSearch.zoneRanges[zoneNum][0] + NeighborSearch.theta
				&& value.dec < NeighborSearch.zoneRanges[zoneNum][1] - NeighborSearch.theta
				&& value.ra > NeighborSearch.blockRanges[raNum][0] + NeighborSearch.maxAlphas[zoneNum]
				&& value.ra < NeighborSearch.blockRanges[raNum][1] - NeighborSearch.maxAlphas[zoneNum])
			return;

		/*
		 * the code below is to copy the star to some neighbors. We only
		 * need to copy an object to the bottom, left, left bottom, left top
		 * neighbors
		 */
		value.margin = true;

		/*
		 * we should treat the entire zone 0 as a block, so we only needs to
		 * copy some objects at the corner to their neighbors
		 */
		if (loc.zoneNum == 0) {
			/* copy the object to the right top neighbor */
			if (value.ra >= NeighborSearch.blockRanges[raNum][1] - NeighborSearch.maxAlphas[zoneNum]
			     && value.ra <= NeighborSearch.blockRanges[raNum][1]
			     && value.dec >= NeighborSearch.zoneRanges[zoneNum][1] - NeighborSearch.theta
			     && value.dec <= NeighborSearch.zoneRanges[zoneNum][1]) {
//				BlockIDWritable loc1 = new BlockIDWritable();
				/* raNum of objects in zone 0 is always 0,
				 * we need to recalculate it. */
//				loc1.raNum = BlockIDWritable.ra2Num(value.ra) + 1;
//				if (loc1.raNum == numBlocks) {
//					loc1.raNum = 0;
//					value.ra -= 360;
//				}
//				loc1.zoneNum = loc.zoneNum + 1;
///					output.collect(loc1, p);
			}
			return;
		} else if (loc.zoneNum == NeighborSearch.numZones - 1) {
			/* copy the object to the bottom neighbor */
			if (value.dec >= NeighborSearch.zoneRanges[zoneNum][0]
			     && value.dec <= NeighborSearch.zoneRanges[zoneNum][0] + NeighborSearch.theta) {
				/* raNum of objects in zone zoneNum - 1 is always 0,
				 * we need to recalculate it. */
				loc1.raNum = BlockIDWritable.ra2Num(value.ra);
				loc1.zoneNum = loc.zoneNum - 1;
				context.write(loc1, p);

				/* copy the object to the right bottom neighbor */
				while (value.ra >= NeighborSearch.blockRanges[loc1.raNum][1] - NeighborSearch.maxAlphas[zoneNum]
				                      							&& value.ra <= NeighborSearch.blockRanges[loc1.raNum][1]) {
					loc1.raNum++;
					if (loc1.raNum == NeighborSearch.numBlocks) {
						loc1.raNum = 0;
						value.ra -= 360;
					}
					loc1.zoneNum = loc.zoneNum - 1;
					context.write(loc1, p);
				}
			}
			return;
		}

		boolean wrap = false;
		loc1.raNum = loc.raNum;
		/* copy the object to the right neighbor */
		while (value.ra >= NeighborSearch.blockRanges[loc1.raNum][1] - NeighborSearch.maxAlphas[zoneNum]
				&& value.ra <= NeighborSearch.blockRanges[loc1.raNum][1]) {
			loc1.raNum++;
			loc1.zoneNum = loc.zoneNum;
			/*
			 * when the object is copied to the right neighbor, we need to
			 * be careful. we need to convert ra and raNum if ra is close to
			 * 360.
			 */
			if (loc1.raNum == NeighborSearch.numBlocks) {
				loc1.raNum = 0;
				value.ra -= 360;
				wrap = true;
			}
			context.write(loc1, p);
			/* copy the object to the right bottom neighbor */
			if (value.dec >= NeighborSearch.zoneRanges[zoneNum][0]
					&& value.dec <= NeighborSearch.zoneRanges[zoneNum][0] + NeighborSearch.theta) {
				loc1.zoneNum = loc.zoneNum - 1;
				context.write(loc1, p);
			}
			/* copy the object to the right top neighbor */
			if (value.dec >= NeighborSearch.zoneRanges[zoneNum][1] - NeighborSearch.theta
					&& value.dec <= NeighborSearch.zoneRanges[zoneNum][1]) {
				loc1.zoneNum = loc.zoneNum + 1;
				context.write(loc1, p);
			}
		}
		if (wrap) {
			value.ra += 360;
		}

		/* copy the object to the bottom neighbor */
		if (value.dec >= NeighborSearch.zoneRanges[zoneNum][0]
				&& value.dec <= NeighborSearch.zoneRanges[zoneNum][0] + NeighborSearch.theta) {
			loc1.raNum = loc.raNum;
			loc1.zoneNum = loc.zoneNum - 1;
			if (loc1.zoneNum == 0)
				loc1.raNum = 0;
			context.write(loc1, p);
		}

	}
}

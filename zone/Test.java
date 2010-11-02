package zone;

public class Test {
	static private final int numZones = 180;
	static private int blockWidth = 1;
	static private int zoneHeight = 1;
	static private final int numBlocks = 360;
	// TODO for different zones, the block width can also be different.
	static private double blockRanges[][] = new double[numBlocks][2];
	// TODO I might need different zone height for different zones
	static private double zoneRanges[][] = new double[numZones][2];
	static private double maxAlphas[] = new double[numZones];
	static private double theta = 0.1;
	
	/**
	 * convert a radian to the block number in the horizon
	 */
	static public int ra2Num(double ra) {
		int num = (int) ra / blockWidth;
		if (ra >= blockRanges[num][0] && ra < blockRanges[num][1])
			return num;
		
		if (num == blockRanges.length && ra == blockRanges[num][1])
			return num;

		/* normally this shouldn't happen. */
		for (int i = num + 1; i < blockRanges.length; i++) {
			if (blockRanges[i][0] >= ra)
				return i;
		}
		for (int i = num - 1; i >= 0; i--) {
			if (blockRanges[i][1] > ra)
				return i;
		}
		return -1;
	}

	/**
	 * convert the latitude to a zone number
	 */
	static public int dec2Num(double dec) {
		int num = (int) ((dec + 90) / zoneHeight);
		if (dec >= zoneRanges[num][0] && dec < zoneRanges[num][1])
			return num;
		
		if (num == zoneRanges.length && dec == zoneRanges[num][1])
			return num;

		/* normally this shouldn't happen. */
		for (int i = num + 1; i < zoneRanges.length; i++) {
			if (zoneRanges[i][0] >= dec)
				return i;
		}
		for (int i = num - 1; i >= 0; i--) {
			if (zoneRanges[i][1] > dec)
				return i;
		}
		return -1;
	}

	static public double calAlpha(double theta, double dec) {
		if (Math.abs(dec) + theta > 89.9)
			return 180;

		return (double) Math.toDegrees(Math.abs(Math.atan(Math.sin(Math
				.toRadians(theta))
				/ Math.sqrt(Math.cos(Math.toRadians(dec - theta))
						* Math.cos(Math.toRadians(dec + theta))))));
	}
	
	public static void collect (Star value, Output output) {
		int raNum = ra2Num(value.ra);
		int zoneNum = dec2Num(value.dec);
		value.zoneNum = zoneNum;
		BlockIDWritable loc = new BlockIDWritable();
		loc.zoneNum = zoneNum;
		loc.raNum = raNum;
		PairWritable p = new PairWritable(value, null);

		/*
		 * When the block size increases (> theta), only part of a block
		 * needs to be copied to its neighbor.
		 */
		output.collect(loc, p);

		/*
		 * only replicate objects in the border of a block. I expect most of
		 * objects don't need to be copied.
		 */
		if (value.dec > zoneRanges[zoneNum][0] + theta
				&& value.dec < zoneRanges[zoneNum][1] - theta
				&& value.ra > blockRanges[raNum][0] + maxAlphas[zoneNum]
				&& value.ra < blockRanges[raNum][1] - maxAlphas[zoneNum])
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
			if (value.ra >= blockRanges[raNum][1] - maxAlphas[zoneNum]
					&& value.ra <= blockRanges[raNum][1]
					&& value.dec >= zoneRanges[zoneNum][1] - theta
					&& value.dec <= zoneRanges[zoneNum][1]) {
				BlockIDWritable loc1 = new BlockIDWritable();
				loc1.raNum = loc.raNum + 1;
				if (loc1.raNum == blockRanges.length) {
					loc1.raNum = 0;
					value.ra -= 360;
				}
				loc1.zoneNum = loc.zoneNum + 1;
				output.collect(loc1, p);
			}
			return;
		} else if (loc.zoneNum == zoneRanges.length - 1) {
			/* copy the object to the bottom neighbor */
			if (value.dec >= zoneRanges[zoneNum][0]
					&& value.dec <= zoneRanges[zoneNum][0] + theta) {
				BlockIDWritable loc1 = new BlockIDWritable();
				loc1.raNum = loc.raNum;
				loc1.zoneNum = loc.zoneNum - 1;
				output.collect(loc1, p);

				/* copy the object to the right bottom neighbor */
				if (value.ra >= blockRanges[raNum][1] - maxAlphas[zoneNum]
						&& value.ra <= blockRanges[raNum][1]) {
					loc1.raNum = loc.raNum + 1;
					if (loc1.raNum == blockRanges.length) {
						loc1.raNum = 0;
						value.ra -= 360;
					}
					loc1.zoneNum = loc.zoneNum - 1;
					output.collect(loc1, p);
				}
			}
			return;
		}

		/* copy the object to the right neighbor */
		if (value.ra >= blockRanges[raNum][1] - maxAlphas[zoneNum]
				&& value.ra <= blockRanges[raNum][1]) {
			BlockIDWritable loc1 = new BlockIDWritable();
			loc1.raNum = loc.raNum + 1;
			/*
			 * when the object is copied to the right neighbor, we need to
			 * be careful. we need to convert ra and raNum if ra is close to
			 * 360.
			 */
			if (loc1.raNum == blockRanges.length) {
				loc1.raNum = 0;
				value.ra -= 360;
			}
			loc1.zoneNum = loc.zoneNum;
			output.collect(loc1, p);
			/* copy the object to the right bottom neighbor */
			if (value.dec >= zoneRanges[zoneNum][0]
					&& value.dec <= zoneRanges[zoneNum][0] + theta) {
				loc1.zoneNum = loc.zoneNum - 1;
				output.collect(loc1, p);
			}
			/* copy the object to the right top neighbor */
			if (value.dec >= zoneRanges[zoneNum][1] - theta
					&& value.dec <= zoneRanges[zoneNum][1]) {
				loc1.zoneNum = loc.zoneNum + 1;
				output.collect(loc1, p);
			}
			if (loc1.raNum == 0) {
				value.ra += 360;
			}
		}

		/* copy the object to the bottom neighbor */
		if (value.dec >= zoneRanges[zoneNum][0]
				&& value.dec <= zoneRanges[zoneNum][0] + theta) {
			BlockIDWritable loc1 = new BlockIDWritable();
			loc1.raNum = loc.raNum;
			loc1.zoneNum = loc.zoneNum - 1;
			if (loc1.zoneNum >= 0)
				output.collect(loc1, p);
		}

	}

	private static class Output {
		public void collect (BlockIDWritable loc, PairWritable pair) {
			System.out.println(loc + ": " + pair);
		}
	}

	public static void main(String args[]) {
		zoneRanges[0][0] = -90;
		zoneRanges[0][1] = -90 + zoneHeight;
		for (int i = 1; i < zoneRanges.length; i++) {
			zoneRanges[i][0] = zoneRanges[i - 1][1];
			zoneRanges[i][1] = zoneRanges[i][0] + zoneHeight;
		}

		blockRanges[0][0] = 0;
		blockRanges[0][1] = blockWidth;
		for (int i = 1; i < blockRanges.length; i++) {
			blockRanges[i][0] = blockRanges[i - 1][1];
			blockRanges[i][1] = blockRanges[i][0] + blockWidth;
		}

		for (int i = 0; i < maxAlphas.length; i++) {
			double maxDec = zoneRanges[i][1] - 90;
			if (maxDec < 0)
				maxDec = zoneRanges[i][0] - 90;
			maxAlphas[i] = calAlpha(theta, maxDec);
		}
		
		Star value = new Star();
		value.dec = 89.1;
		value.ra = 359.90000000001;
		Output output = new Output();
		collect (value, output);
	}

}

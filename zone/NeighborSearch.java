package zone;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import zone.io.StarInputFormat;
import zone.io.StarOutputFormat;

/* export data from a SQL server
 * bcp "SELECT TOP 100000 * FROM [BestDR7].[dbo].[Zone]" queryout zone100000 -n -Sgw20 -T */

public class NeighborSearch {
	static public final int numZones = 180;
	static public final int numBlocks = 360;
	static public double theta = 1.0/6.0;
	static public double blockWidth = 360.0 / numBlocks;
	static public double zoneHeight = 180.0 / numZones;
	static private double blockRanges[][] = new double[numBlocks][2];
	static private double zoneRanges[][] = new double[numZones][2];
	static private double maxAlphas[] = new double[numZones];
	static private double costheta = Math.cos(Math.toRadians(theta));
	
	static public double calAlpha(double theta, double dec) {
		if (Math.abs(dec) + theta > 89.9)
			return 180;

		return (double) Math.toDegrees(Math.abs(Math.atan(Math.sin(Math
				.toRadians(theta))
				/ Math.sqrt(Math.cos(Math.toRadians(dec - theta))
						* Math.cos(Math.toRadians(dec + theta))))));
	}
	
	public static void init() {
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
			double maxDec = zoneRanges[i][1];
			if (maxDec <= 0)
				maxDec = zoneRanges[i][0];
			maxAlphas[i] = calAlpha(theta, maxDec);
		}
	}

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Star, BlockIDWritable, PairWritable> {
		
		/* it seems it's very costly to create an object in Java.
		 * reuse these objects in every map invocation. */
		private BlockIDWritable loc = new BlockIDWritable();
		private PairWritable p = new PairWritable();
		BlockIDWritable loc1 = new BlockIDWritable();
		
		public Map() {
			init();
		}

		public void map(LongWritable key, Star value,
				OutputCollector<BlockIDWritable, PairWritable> output,
				Reporter reporter) throws IOException {

			loc.set(value.ra, value.dec);
			int zoneNum = loc.zoneNum;
			int raNum = loc.raNum;
			p.set(value, null);

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
//					BlockIDWritable loc1 = new BlockIDWritable();
					/* raNum of objects in zone 0 is always 0,
					 * we need to recalculate it. */
//					loc1.raNum = BlockIDWritable.ra2Num(value.ra) + 1;
//					if (loc1.raNum == numBlocks) {
//						loc1.raNum = 0;
//						value.ra -= 360;
//					}
//					loc1.zoneNum = loc.zoneNum + 1;
///					output.collect(loc1, p);
				}
				return;
			} else if (loc.zoneNum == numZones - 1) {
				/* copy the object to the bottom neighbor */
				if (value.dec >= zoneRanges[zoneNum][0]
				             						&& value.dec <= zoneRanges[zoneNum][0] + theta) {
					/* raNum of objects in zone zoneNum - 1 is always 0,
					 * we need to recalculate it. */
					loc1.raNum = BlockIDWritable.ra2Num(value.ra);
					loc1.zoneNum = loc.zoneNum - 1;
					output.collect(loc1, p);

					/* copy the object to the right bottom neighbor */
					while (value.ra >= blockRanges[loc1.raNum][1] - maxAlphas[zoneNum]
					                      							&& value.ra <= blockRanges[loc1.raNum][1]) {
						loc1.raNum++;
						if (loc1.raNum == numBlocks) {
							loc1.raNum = 0;
							value.ra -= 360;
						}
						loc1.zoneNum = loc.zoneNum - 1;
						output.collect(loc1, p);
					}
				}
				return;
			}

			boolean wrap = false;
			loc1.raNum = loc.raNum;
			/* copy the object to the right neighbor */
			while (value.ra >= blockRanges[loc1.raNum][1] - maxAlphas[zoneNum]
					&& value.ra <= blockRanges[loc1.raNum][1]) {
				loc1.raNum++;
				loc1.zoneNum = loc.zoneNum;
				/*
				 * when the object is copied to the right neighbor, we need to
				 * be careful. we need to convert ra and raNum if ra is close to
				 * 360.
				 */
				if (loc1.raNum == numBlocks) {
					loc1.raNum = 0;
					value.ra -= 360;
					wrap = true;
				}
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
			}
			if (wrap) {
				value.ra += 360;
			}

			/* copy the object to the bottom neighbor */
			if (value.dec >= zoneRanges[zoneNum][0]
					&& value.dec <= zoneRanges[zoneNum][0] + theta) {
				loc1.raNum = loc.raNum;
				loc1.zoneNum = loc.zoneNum - 1;
				if (loc1.zoneNum == 0)
					loc1.raNum = 0;
				output.collect(loc1, p);
			}

		}
	}

	public static class Reduce extends MapReduceBase
			implements
			Reducer<BlockIDWritable, PairWritable, BlockIDWritable, PairWritable> {
		
		public Reduce() {
			init();
		}
		
		void search(Vector<Star> v1, Vector<Star> v2, BlockIDWritable key, 
				OutputCollector<BlockIDWritable, PairWritable> output) throws IOException {
			for (int i = 0; i < v1.size(); i++) {
				for (int j = 0; j < v2.size(); j++) {
					Star star1 = v1.get(i);
					Star star2 = v2.get(j);
					//what is this margin about
					if (star1.margin && star2.margin)
						continue;

					if (star1.ra >= star2.ra - maxAlphas[key.zoneNum]
							&& star1.ra <= star2.ra + maxAlphas[key.zoneNum]
							&& star1.dec >= star2.dec - theta
							&& star1.dec <= star2.dec + theta
							&& star1.x * star2.x + star1.y * star2.y + star1.z
									* star2.z > Math.cos(Math.toRadians(theta))) {
						output.collect(key, new PairWritable(star1, star2));
						output.collect(key, new PairWritable(star2, star1));
				//		num += 2;
						
					}
				}
			}//end for i,j
		}
		
		public void reduce(BlockIDWritable key, Iterator<PairWritable> values,
				OutputCollector<BlockIDWritable, PairWritable> output,
				Reporter reporter) throws IOException {
			//Vector<Star> starV = new Vector<Star>();
			int buketsizeX=0;
			int buketsizeY=0;
			double bwidth=maxAlphas[key.zoneNum]; //ra ,x
			double bheight=theta; //dec ,y
			/* add 10 more in each dimension to make sure there is no overflow. */
			Vector<Star> [][] arrstarV=new Vector[((int) (zoneHeight
						/ bheight)) + 10][((int) (blockWidth / bwidth)) + 10]; //create bucket vector[Y][X]
			
			int num = 0;
			while (values.hasNext()) {
				num++;
				Star s = values.next().get(0);
				
				//participant
				double posx= (s.ra-blockRanges[key.raNum][0])/bwidth;
				int x=(int)posx+1; //shit by 1 in case star comes from other block
				double posy= (s.dec-zoneRanges[key.zoneNum][0])/bheight;
				int y=(int)posy+1;
				
				//set bucket size as max
				if(buketsizeX<x)
					buketsizeX=x;
				if(buketsizeY<y)
					buketsizeY=y;
				//create according bucket
				if(arrstarV[y][x]==null)
					arrstarV[y][x]=new Vector<Star>();
				//put star into bucket
				arrstarV[y][x].add(s);
			}
			// start reducer
			int i,j,row, col;
			//for each bucket
			for(row=0;row<=buketsizeY;row++)
			{
				for(col=0;col<=buketsizeX;col++)
				{
			//		starV.clear();
					//construct a new vector to do compare
					// TODO we need to avoid searching objects in the border.
					if(arrstarV[row][col]!=null)
					{
						//old method to generate output
						for (i = 0; i < arrstarV[row][col].size(); i++) {
							for (j = i + 1; j < arrstarV[row][col].size(); j++) {
								Star star1 = arrstarV[row][col].get(i);
								Star star2 = arrstarV[row][col].get(j);
								//what is this margin about
								if (star1.margin && star2.margin)
									continue;
	
								double dist = star1.x * star2.x + star1.y * star2.y + star1.z * star2.z;
								if (star1.ra >= star2.ra - maxAlphas[key.zoneNum]
										&& star1.ra <= star2.ra + maxAlphas[key.zoneNum]
										&& star1.dec >= star2.dec - theta
										&& star1.dec <= star2.dec + theta
										&& dist > costheta) {
									output.collect(key, new PairWritable(star1, star2, dist));
									output.collect(key, new PairWritable(star2, star1, dist));
							//		num += 2;
									
								}
							}
						}//end for i,j
					
					}//end if
					else {
						continue;
					}
					//4 more neighbors
					//right upper arrstarV[row-1][col+1] vs arrstarV[row][col]
					if(row!=0 && arrstarV[row-1][col+1]!=null) 
					{
						search(arrstarV[row][col], arrstarV[row-1][col+1], key, output);
					}
					//right arrstarV[row][col+1] vs arrstarV[row][col]
					if(arrstarV[row][col+1]!=null)
					{
						search(arrstarV[row][col], arrstarV[row][col+1], key, output);
					}
					//right lower
					if(arrstarV[row+1][col+1]!=null)
					{
						search(arrstarV[row][col], arrstarV[row+1][col+1], key, output);
					}
					//lower
					if(arrstarV[row+1][col]!=null)
					{
						search(arrstarV[row][col], arrstarV[row+1][col], key, output);
					}//end if
				}//end colum
			}//end row
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(NeighborSearch.class);
		conf.setJobName("star searching");

		conf.setOutputKeyClass(BlockIDWritable.class);
		conf.setOutputValueClass(PairWritable.class);

		conf.setMapperClass(Map.class);
		// conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setPartitionerClass(BlockPartitioner.class);

		conf.setInputFormat(StarInputFormat.class);
		conf.setOutputFormat(StarOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}

}

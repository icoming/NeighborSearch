package zone.newapi.stat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.mapreduce.Reducer;

import zone.BlockIDWritable;
import zone.Star;
import zone.newapi.NeighborSearch;

public class ReduceClass extends Reducer<BlockIDWritable, Star, BlockIDWritable, LongArrayWritable> { 
	static final int numStat = 60;
	
	Vector<Star> [][] arrstarV;
	
	public ReduceClass() {
		NeighborSearch.init();

		double bwidth=NeighborSearch.theta; // theta is smaller than any alpha
		double bheight=NeighborSearch.theta; //dec ,y
		int x = ((int) (NeighborSearch.zoneHeight / bheight)) + 10;
		int y = ((int) (NeighborSearch.blockWidth / bwidth)) + 10;
		/* add 10 more in each dimension to make sure there is no overflow. */
		arrstarV=new Vector[x][y]; //create bucket vector[Y][X]
		
		for (int i = 0; i < x; i++) {
			for (int j = 0; j < y; j++)
				arrstarV[i][j] = new Vector<Star>();
		}
	}
	
	void search(Vector<Star> v1, Vector<Star> v2, BlockIDWritable key, 
			LongArrayWritable arr) throws IOException, InterruptedException {
		for (int i = 0; i < v1.size(); i++) {
			for (int j = 0; j < v2.size(); j++) {
				Star star1 = v1.get(i);
				Star star2 = v2.get(j);
				//what is this margin about
				if (star1.margin && star2.margin)
					continue;

				// it's in arcseconds.
				double dist = Math.toDegrees(Math.acos(star1.x
						* star2.x + star1.y * star2.y + star1.z * star2.z)) * 3600;
				if (dist >= numStat)
					continue;
				for (int k = (int) dist; k < numStat; k++)
					arr.inc(k + 1);
			}
		}//end for i,j
	}

	@Override
	public void reduce(BlockIDWritable key, Iterable<Star> values,
			Context context) throws IOException, InterruptedException {
		//Vector<Star> starV = new Vector<Star>();
		int buketsizeX=0;
		int buketsizeY=0;
		double bwidth=NeighborSearch.maxAlphas[key.zoneNum]; //ra ,x
		double bheight=NeighborSearch.theta; //dec ,y
		
		int num = 0;
		Iterator<Star> it = values.iterator();
		while (it.hasNext()) {
			num++;
			Star s = it.next();
			// the iterator always uses the same star object. 
			// so I need to make a copy when I get a new star
			s = s.clone();
			
			//participant
			double posx= (s.ra-NeighborSearch.blockRanges[key.raNum][0])/bwidth;
			int x=(int)posx+1; //shit by 1 in case star comes from other block
			double posy= (s.dec-NeighborSearch.zoneRanges[key.zoneNum][0])/bheight;
			int y=(int)posy+1;
			
			//set bucket size as max
			if(buketsizeX<x)
				buketsizeX=x;
			if(buketsizeY<y)
				buketsizeY=y;
			//put star into bucket
			arrstarV[y][x].add(s);
		}
		
		// we need an extra entry to store the number of input elements
		LongArrayWritable arr = new LongArrayWritable(numStat + 1);
		arr.set(0, num);
		
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
				if(arrstarV[row][col].size() > 0)
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
							// it's in arcseconds.
							dist = Math.toDegrees(Math.acos(dist)) * 3600;
							if (dist >= numStat)
								continue;
							for (int k = (int) dist; k < numStat; k++)
								arr.inc(k + 1);
						}
					}//end for i,j
				
				}//end if
				else {
					continue;
				}
				//4 more neighbors
				//right upper arrstarV[row-1][col+1] vs arrstarV[row][col]
				if(row!=0) 
				{
					search(arrstarV[row][col], arrstarV[row-1][col+1], key, arr);
				}
				//right arrstarV[row][col+1] vs arrstarV[row][col]
				search(arrstarV[row][col], arrstarV[row][col+1], key, arr);
				//right lower
				search(arrstarV[row][col], arrstarV[row+1][col+1], key, arr);
				//lower
				search(arrstarV[row][col], arrstarV[row+1][col], key, arr);
			}//end colum
		}//end row
		context.write(key, arr);
		
		/* clean up all vectors */
		for(row=0;row<=buketsizeY;row++)
		{
			for(col=0;col<=buketsizeX;col++)
			{
				arrstarV[row][col].clear();
			}
		}
	}
}

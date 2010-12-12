package zone.newapi;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.mapreduce.Reducer;

import zone.BlockIDWritable;
import zone.PairWritable;
import zone.Star;

public class ReduceClass extends Reducer<BlockIDWritable, Star, BlockIDWritable, PairWritable> { 
	PairWritable p = new PairWritable();
	
	public ReduceClass() {
		NeighborSearch.init();
	}
	
	void search(Vector<Star> v1, Vector<Star> v2, BlockIDWritable key, 
			Context context) throws IOException, InterruptedException {
		for (int i = 0; i < v1.size(); i++) {
			for (int j = 0; j < v2.size(); j++) {
				Star star1 = v1.get(i);
				Star star2 = v2.get(j);
				//what is this margin about
				if (star1.margin && star2.margin)
					continue;

				double dist = star1.x * star2.x + star1.y * star2.y + star1.z * star2.z;
				if (dist > NeighborSearch.costheta) {
					p.set (star1, star2, dist);
					context.write(key, p);
					p.set (star2, star1, dist);
					context.write(key, p);
			//		num += 2;
					
				}
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
		/* add 10 more in each dimension to make sure there is no overflow. */
		Vector<Star> [][] arrstarV=new Vector[((int) (NeighborSearch.zoneHeight
					/ bheight)) + 10][((int) (NeighborSearch.blockWidth / bwidth)) + 10]; //create bucket vector[Y][X]
		
		int num = 0;
		Iterator<Star> it = values.iterator();
		while (it.hasNext()) {
			num++;
			Star s = it.next();
			
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
			//create according bucket
			if(arrstarV[y][x]==null)
				// TODO avoid creating vectors here.
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
							if (dist > NeighborSearch.costheta) {
								p.set(star1, star2, dist);
								context.write(key, p);
								p.set(star2, star1, dist);
								context.write(key, p);
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
					search(arrstarV[row][col], arrstarV[row-1][col+1], key, context);
				}
				//right arrstarV[row][col+1] vs arrstarV[row][col]
				if(arrstarV[row][col+1]!=null)
				{
					search(arrstarV[row][col], arrstarV[row][col+1], key, context);
				}
				//right lower
				if(arrstarV[row+1][col+1]!=null)
				{
					search(arrstarV[row][col], arrstarV[row+1][col+1], key, context);
				}
				//lower
				if(arrstarV[row+1][col]!=null)
				{
					search(arrstarV[row][col], arrstarV[row+1][col], key, context);
				}//end if
			}//end colum
		}//end row
	}
}

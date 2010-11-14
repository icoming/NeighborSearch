package zone.util;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import zone.NeighborSearch;
import zone.Star;

public class Dist {
	public static void main(String args[]) {
		double cos = Math.cos(Math.toRadians(NeighborSearch.theta));
		Long ids[] = new Long[2];
		for (int i = 1; i < args.length; i++) {
			ids[i - 1] = new Long(args[i]);
		}
		Star stars[] = new Star[2];
		for (int i = 0; i < 2; i++)
			stars[i] = Star.createStar();
		try {
			DataInputStream in = new DataInputStream(new BufferedInputStream (new FileInputStream (args[0])));
			int num = 0;
			while (true) {
				stars[num].set(in);
				int i;
				for (i = 0; i < ids.length; i++) {
					if (stars[num].objID == ids[i]) {
						System.out.print("find it @"
								+ new zone.BlockIDWritable(stars[num].ra, stars[num].dec) + ": ");
						stars[num].print();
						num++;
						break;
					}
				}
				
				if (num == 2)
					break;
			}
			System.out.println("theta: " + NeighborSearch.theta);
			System.out.println("theta dist: " + Math.abs(stars[0].dec - stars[1].dec));
			double dist = stars[0].x * stars[1].x + stars[0].y * stars[1].y + stars[0].z * stars[1].z;
			System.out.println("dist: " + dist);
			System.out.println("needs " + cos);
			System.out.println("alpha0: " + NeighborSearch.calAlpha (NeighborSearch.theta, stars[0].dec));
			System.out.println("alpha1: " + NeighborSearch.calAlpha (NeighborSearch.theta, stars[1].dec));
			System.out.println("small? " + (dist < cos));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

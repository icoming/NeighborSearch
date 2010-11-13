package zone.util;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import zone.Star;

public class Search {
	public static void main(String args[]) {
		Long ids[] = new Long[args.length - 1];
		for (int i = 1; i < args.length; i++) {
			ids[i - 1] = new Long(args[i]);
		}
		Star star = Star.createStar();
		double ras[] = null;
		try {
			DataInputStream in = new DataInputStream(new BufferedInputStream (new FileInputStream (args[0])));
			for (int j = 0; in.available() >= star.size(); j++) {
				star.set(in);
				if (ras == null) {
					ras = new double[2];
					ras[0] = star.ra;
					ras[1] = star.ra;
				}
				else {
					if (star.ra > ras[1])
						ras[1] = star.ra;
					if (star.ra < ras[0])
						ras[0] = star.ra;
				}
//				star.print();
//				if (j > 1000)
//					return;
				int i;
				for (i = 0; i < ids.length; i++) {
					if (star.objID == ids[i]) {
						System.out.print("find it@" + j + ": ");
						star.print();
						break;
					}
				}
				
			}
			System.out.println("ra: " + ras[0] + "-" + ras[1]);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

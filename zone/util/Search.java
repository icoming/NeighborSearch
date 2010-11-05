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
		Star star = new Star();
		try {
			DataInputStream in = new DataInputStream(new BufferedInputStream (new FileInputStream (args[0])));
			for (int j = 0; in.available() >= Star.storeSize; j++) {
				star.set(in);
				int i;
				for (i = 0; i < ids.length; i++) {
					if (star.objID == ids[i]) {
						System.out.print("find it@" + j + ": ");
						star.print();
						break;
					}
				}
				
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

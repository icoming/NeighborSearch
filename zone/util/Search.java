package zone.util;

import java.io.BufferedInputStream;
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
		byte bytes[] = new byte[Star.storeSize];
		Star star = new Star();
		try {
			BufferedInputStream in = new BufferedInputStream (new FileInputStream (args[0]));
			while (true) {
				int size = in.read(bytes);
				if (size == -1) {
//					System.out.println("cannot find the star");
					return;
				}
				
				star.set(bytes);
				int i;
				for (i = 0; i < ids.length; i++) {
					if (star.objID == ids[i]) {
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

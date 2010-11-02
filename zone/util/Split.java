package zone.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import zone.Star;

public class Split {
	private static int splitSize = 50000;	// the number of record
	public static void main (String[] args) {
		try {
			int num = 0;
			FileInputStream in = new FileInputStream(args[0]);
			byte[] bytes = new byte[Star.storeSize];
			boolean end = false;
			while (!end) {
				FileOutputStream out = new FileOutputStream(args[1] + "-" + num);
				num++;
				for (int i = 0; i < splitSize; i++) {
					int size = in.read(bytes);
					if (size < bytes.length) {
						end = true;
						break;
					}
					out.write(bytes);
				}
				out.close();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

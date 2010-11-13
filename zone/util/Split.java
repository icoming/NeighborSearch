package zone.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import zone.Star;

public class Split {
	private static final int blockSize = 32000000;
	private static final int goalNumSplits = 4;
	public static void main (String[] args) {
		try {
			int num = 0;
			File file = new File(args[0]);
			int splitSize;	// the number of records
			int storeSize = Star.createStar().size();
			splitSize = (int) ((file.length() + storeSize - 1) / storeSize / goalNumSplits);
			if (splitSize > blockSize / storeSize)
				splitSize = blockSize / storeSize;
			
			FileInputStream in = new FileInputStream(args[0]);
			byte[] bytes = new byte[storeSize];
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
				if (in.available() == 0)
					break;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

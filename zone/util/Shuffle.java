package zone.util;

import java.util.Random;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import zone.Star;

public class Shuffle {
	public static void main (String[] args) {
		try {
			int num = 0;
			int storeSize = Star.createStar().size();
			File file = new File(args[0]);
			FileInputStream in = new FileInputStream(args[0]);
			FileOutputStream out = new FileOutputStream(args[1]);
			int nShuffles = new Integer(args[2]).intValue();
			byte[] bytes = new byte[(int) file.length()];
			byte[] tmp = new byte[storeSize];
			int nRecords = bytes.length / storeSize;
			System.out.println("there are " + nRecords + "records");
			in.read(bytes);
			Random r = new Random();
			for (int i = 0; i < nShuffles; i++) {
				int rand1 = r.nextInt(nRecords);
				int rand2 = r.nextInt(nRecords);
				if (rand1 == rand2)
					continue;
				System.arraycopy(bytes, rand2 * storeSize, tmp, 0, storeSize);
				System.arraycopy(bytes, rand1 * storeSize, bytes,
						rand2 * storeSize, storeSize);
				System.arraycopy(tmp, 0, bytes, rand1 * storeSize, storeSize);
			}
			out.write(bytes);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}


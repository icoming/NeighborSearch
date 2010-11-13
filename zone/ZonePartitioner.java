package zone;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.JobConf;

/**
 * partition the output data of a mapper according to its zone number
 * @author zhengda
 *
 * @param <K2>
 * @param <V2>
 */
public class ZonePartitioner<K2, V2> implements Partitioner<K2, V2> {
	private int[] parts;
	private int numReduces;
	private double height;

	public void configure(JobConf job) {
		Path file = new Path("/user/zhengda/zone-sample/part-00000");
		FileSystem fs;
		numReduces = job.getNumReduceTasks();
		parts = new int[numReduces + 1];
		ArrayList<Integer> zones = new ArrayList<Integer>();
		ArrayList<Integer> sizes = new ArrayList<Integer>();
		try {
			fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(file);
			int tot = 0;
			while(fileIn.available() > 0) {
				String[] strs = fileIn.readLine().split("\t");
				zones.add(new Integer(strs[0]));
				Integer n = new Integer(strs[1]);
				tot += n.intValue();
				sizes.add(n);
			}
			int partSize = tot / numReduces;
			parts[0] = zones.get(0);
			System.out.println("part " + parts[0]);
			parts[parts.length - 1] = zones.get(zones.size() - 1);
			int n = 1;
			for (int i = 1; i < zones.size(); i++) {
				if (partSize <= 0) {
					partSize = tot / numReduces;
					parts[n] = zones.get(i);
					System.out.println("part " + parts[n]);
					n++;
				}
				partSize -= sizes.get(i);
			}
			System.out.println("part " + parts[parts.length - 1]);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		height = ((double) NeighborSearch.numZones) / numReduces;
	}

	/** Use {@link Object#hashCode()} to partition. */
	public int getPartition(K2 key, V2 value,
			int numReduceTasks) {
		int partition = 0;
		for (; partition < numReduces; partition++) {
			if (key.hashCode() >= parts[partition] && key.hashCode() < parts[partition + 1])
				break;
		}
		if (partition >= numReduces)
			partition = numReduces - 1;
//		System.out.println("hash " + key.hashCode() + " --> " + partition);
		return partition;
	}

}


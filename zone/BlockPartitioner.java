package zone;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * partition the data according to its block ID
 * @author zhengda
 *
 */
public class BlockPartitioner implements Partitioner<BlockIDWritable, PairWritable>  {
	private int numReduces;
	private BlockIDWritable[] parts;

	@Override
	public int getPartition(BlockIDWritable key, PairWritable value, int numReduceTasks) {
		int partition = 0;
		for (; partition < numReduces; partition++) {
			if (key.compareTo(parts[partition]) >= 0 && key.compareTo(parts[partition + 1]) < 0)
				break;
		}
		if (partition >= numReduces)
			partition = numReduces - 1;
//		System.out.println("hash " + key.hashCode() + " --> " + partition);
		return partition;
	}

	@Override
	public void configure(JobConf job) {
		Path file = new Path("/user/zhengda/block-sample/part-00000");
		FileSystem fs;
		numReduces = job.getNumReduceTasks();
		parts = new BlockIDWritable[numReduces + 1];
		ArrayList<BlockIDWritable> zones = new ArrayList<BlockIDWritable>();
		ArrayList<Long> sizes = new ArrayList<Long>();
		try {
			fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(file);
			long tot = 0;
			while(fileIn.available() > 0) {
				BlockIDWritable id = new BlockIDWritable();
				id.readFields(fileIn);
				zones.add(id);
				Long n = new Long(fileIn.readLong());
				tot += n.longValue();
				sizes.add(n);
			}
			long partSize = tot / numReduces;
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
	}

}

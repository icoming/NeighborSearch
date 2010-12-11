package zone.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.net.NetworkTopology;

import zone.Star;

public class StarInputFormat extends FileInputFormat<LongWritable, Star> {
	
	@Override
	public org.apache.hadoop.mapred.RecordReader<LongWritable, Star> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter) throws IOException {
	    reporter.setStatus(split.toString());
		return new StarReader(job, (FileSplit) split);
	}
	
	private class SplitInfo {
		public long totLen;
		public long splitLen;
		public SplitInfo() {
			totLen = 0;
			splitLen = 0;
		}
	}

	private static final double SPLIT_SLOP = 1.1;   // 10% slop

	 /** Splits files returned by {@link #listStatus(JobConf)} when
	   * they're too big.*/
	 public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		FileStatus[] files = listStatus(job);

		long totalSize = 0; // compute total size
		for (FileStatus file : files) { // check we have valid files
			if (file.isDir()) {
				throw new IOException("Not a file: " + file.getPath());
			}
			totalSize += file.getLen();
		}

		long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
		long minSize = job.getLong("mapred.min.split.size", 1);

		// generate splits
		ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
		NetworkTopology clusterMap = new NetworkTopology();
		int starSize = Star.createStar().size();
		for (FileStatus file : files) {
			Path path = file.getPath();
			FileSystem fs = path.getFileSystem(job);
			long length = file.getLen();
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0,
					length);
			if ((length != 0) && isSplitable(fs, path)) {
				long blockSize = file.getBlockSize();
				long splitSize = computeSplitSize(goalSize, minSize, blockSize);
				splitSize = (splitSize / starSize) * starSize; 

				long bytesRemaining = length;
				while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
					String[] splitHosts = getSplitHosts(blkLocations, length
							- bytesRemaining, splitSize, clusterMap);
					
					FileSplit split = new FileSplit(path, length - bytesRemaining,
							splitSize, splitHosts);

					System.out.print("file " + path.getName() + "("
							+ (length - bytesRemaining) + "+" + splitSize
							+ ")" + " exists in " + split.getLocations().length + " nodes: ");
					for (int i = 0; i < split.getLocations().length; i++) {
						System.out.print(split.getLocations()[i]);
					}
					System.out.println();
					
					splits.add(split);
					bytesRemaining -= splitSize;
				}

				if (bytesRemaining != 0) {
					FileSplit split = new FileSplit(path, length - bytesRemaining,
							bytesRemaining,
							blkLocations[blkLocations.length - 1].getHosts());
					System.out.print("file " + path.getName() + "("
							+ (length - bytesRemaining) + "+" + bytesRemaining
							+ ")" + " exists in " + split.getLocations().length + " nodes: ");
					for (int i = 0; i < split.getLocations().length; i++) {
						System.out.print(split.getLocations()[i]);
					}
					System.out.println();

					splits.add(split);
				}
			} else if (length != 0) {
				String[] splitHosts = getSplitHosts(blkLocations, 0, length,
						clusterMap);
				splits.add(new FileSplit(path, 0, length, splitHosts));
			} else {
				// Create empty hosts array for zero length files
				splits.add(new FileSplit(path, 0, length, new String[0]));
			}
		}
		System.out.println("split into " + splits.size() + " parts");
		for (int i = 0; i < splits.size(); i++) {
			System.out.println("split" + i + ": start " + splits.get(i).getStart()
					+ ", end " + (splits.get(i).getStart() + splits.get(i).getLength()));
		}
		return splits.toArray(new FileSplit[splits.size()]);
	  }

}

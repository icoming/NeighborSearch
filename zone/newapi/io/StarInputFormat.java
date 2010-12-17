package zone.newapi.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import zone.Star;

public class StarInputFormat extends FileInputFormat<LongWritable, Star> {
	private static final double SPLIT_SLOP = 1.1;   // 10% slop

	@Override
	public RecordReader<LongWritable, Star> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new StarReader();
	}

	  @Override
	  protected boolean isSplitable(JobContext context, Path file) {
	    CompressionCodec codec = 
	      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
	    return codec == null;
	  }


	  protected int getBlockIndex(BlockLocation[] blkLocations, long offset,
			  long length) {
		  long end = offset + length;
		  for (int i = 0; i < blkLocations.length; i++) {
			  // is the offset inside this block?
			  if ((blkLocations[i].getOffset() <= offset)
					  && (offset < blkLocations[i].getOffset()
						  + blkLocations[i].getLength())) {
				  if (i == blkLocations.length - 1)
					  return i;
				  // I assume the location of the next block is at i+1
				  // if the end of the split is in the next block
				  else if ((blkLocations[i + 1].getOffset() < end)
						  && (end < blkLocations[i + 1].getOffset()
							  + blkLocations[i + 1].getLength())
						  // if more data is in the next block
						  && end - blkLocations[i + 1].getOffset() > blkLocations[i]
						  .getOffset()
						  + blkLocations[i].getLength() - offset) {
					  return i + 1;
						  }
				  else
					  return i;
						  }
		  }
		  BlockLocation last = blkLocations[blkLocations.length - 1];
		  long fileLength = last.getOffset() + last.getLength() - 1;
		  throw new IllegalArgumentException("Offset " + offset
				  + " is outside of file (0.." + fileLength + ")");
	  }


	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		long maxSize = getMaxSplitSize(job);

		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		int starSize = Star.createStar().size();
		for (FileStatus file : listStatus(job)) {
			Path path = file.getPath();
			FileSystem fs = path.getFileSystem(job.getConfiguration());
			long length = file.getLen();
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0,
					length);
			if ((length != 0) && isSplitable(job, path)) {
				long blockSize = file.getBlockSize();
				// TODO is this really a right way? 
				// when I use setMinInputSplitSize and setMaxInputSplitSize,
				// I get a different number of mapping output records, 
				// but the same number of mapping input records. why?
				long splitSize = computeSplitSize(blockSize, minSize, maxSize);
				splitSize = (splitSize / starSize) * starSize; 

				long bytesRemaining = length;
				while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
					int blkIndex = getBlockIndex(blkLocations, length
							- bytesRemaining, splitSize);
					splits.add(new FileSplit(path, length - bytesRemaining,
							splitSize, blkLocations[blkIndex].getHosts()));
					bytesRemaining -= splitSize;
				}

				if (bytesRemaining != 0) {
					splits.add(new FileSplit(path, length - bytesRemaining,
							bytesRemaining,
							blkLocations[blkLocations.length - 1].getHosts()));
				}
			} else if (length != 0) {
				splits.add(new FileSplit(path, 0, length, blkLocations[0]
						.getHosts()));
			} else {
				// Create empty hosts array for zero length files
				splits.add(new FileSplit(path, 0, length, new String[0]));
			}
		}
		return splits;
	}
}

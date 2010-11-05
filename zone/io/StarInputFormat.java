package zone.io;

import java.io.IOException;
import java.util.ArrayList;

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
	
	// TODO I need to rewrite this.
	 /** Splits files returned by {@link #listStatus(JobConf)} when
	   * they're too big.*/
	 public InputSplit[] getSplits(JobConf job, int numSplits)
	    throws IOException {
	    FileStatus[] files = listStatus(job);
	    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
	
	    long totalSize = 0;                           // compute total size
	    for (FileStatus file: files) {                // check we have valid files
	      if (file.isDir()) {
	        throw new IOException("Not a file: "+ file.getPath());
	      }
	      splits.add(new FileSplit(file.getPath(), 0, file.getLen(), job));
	    }

	    return splits.toArray(new FileSplit[splits.size()]);
	  }

}

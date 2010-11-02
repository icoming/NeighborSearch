package zone.io;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapred.FileInputFormat;

import zone.Star;

public class StarInputFormat extends FileInputFormat<LongWritable, Star> {

	@Override
	public org.apache.hadoop.mapred.RecordReader<LongWritable, Star> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter) throws IOException {
	    reporter.setStatus(split.toString());
		return new StarReader(job, (FileSplit) split);
	}

}

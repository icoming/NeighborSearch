package zone.newapi.io;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import zone.Star;

public class StarReader extends RecordReader<LongWritable, Star> {
	private long start;
	private long pos;
	private long end;
	private CompressionCodecFactory compressionCodecs = null;
	private DataInputStream in;
	private LongWritable key = null;
	private Star value = null;

	@Override
	public void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Star getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
	    FileSplit split = (FileSplit) genericSplit;
	    Configuration job = context.getConfiguration();
	    start = split.getStart();
	    end = start + split.getLength();
		System.out.println("start: " + start + ", end: " + end);
		
	    final Path file = split.getPath();
	    
		System.out.print("the split is in " + file.getName() + ", located in " + split.getLocations().length + " nodes");
		for (int i = 0; i < split.getLocations().length; i++) {
			System.out.print(split.getLocations()[i] + " ");
		}
		System.out.println();
		
	    compressionCodecs = new CompressionCodecFactory(job);
	    final CompressionCodec codec = compressionCodecs.getCodec(file);

	    // open the file and seek to the start of the split
	    FileSystem fs = file.getFileSystem(job);
	    FSDataInputStream fileIn = fs.open(split.getPath());
	    if (codec != null) {
	      in = new DataInputStream(codec.createInputStream(fileIn));
	      end = Long.MAX_VALUE;
	    } else {
	      if (start != 0) {
	        fileIn.seek(start);
	      }
	      in = fileIn;
	    }
	    this.pos = start;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = Star.createStar();
		}
		if (pos >= end || in.available() < value.size()) {
			key = null;
			value = null;
			return false;
		}
		
		value.set(in);
		pos += value.size();
		return true;
	}

}

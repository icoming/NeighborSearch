package zone.io;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import zone.Star;

public class StarReader implements RecordReader<LongWritable, Star> {
	private long start;
	private long pos;
	private long end;
	private CompressionCodecFactory compressionCodecs = null;
	private InputStream in;

	public StarReader(Configuration job, 
	                          FileSplit split) throws IOException {
		// TODO the start point should be recalculated.
		// Should I re-implement FileSplit to make sure the start point is the
		// start of a new object
		start = split.getStart();
		end = start + split.getLength();
		System.out.println("start: " + start + ", end: " + end);
		final Path file = split.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		if (codec != null) {
			in = codec.createInputStream(fileIn);
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
	public LongWritable createKey() {
	    return new LongWritable();
	}

	@Override
	public Star createValue() {
		return new Star();
	}

	@Override
	public long getPos() throws IOException {
		return pos;
	}

	@Override
	public boolean next(LongWritable key, Star value) throws IOException {
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new Star();
		}
		if (pos >= end || in.available() < Star.storeSize) {
			key = null;
			value = null;
			return false;
		}
		
		value.set(new DataInputStream(in));
		pos += Star.storeSize;
		return true;
	}

	@Override
	public float getProgress() throws IOException {
	    if (start == end) {
	        return 0.0f;
	      } else {
	        return Math.min(1.0f, (pos - start) / (float)(end - start));
	      }
	}

	@Override
	public void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}

}

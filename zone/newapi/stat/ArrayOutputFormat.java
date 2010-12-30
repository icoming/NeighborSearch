package zone.newapi.stat;

import java.io.DataOutputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import zone.BlockIDWritable;

public class ArrayOutputFormat extends FileOutputFormat<BlockIDWritable, IntArrayWritable> {

	protected static class BlockRecordWriter extends RecordWriter<BlockIDWritable, IntArrayWritable> {
		private DataOutputStream out;
		
		public BlockRecordWriter (DataOutputStream out) {
			this.out = out;
		}

		@Override
		public void close(TaskAttemptContext arg0) throws IOException,
				InterruptedException {
			out.close();
		}

		@Override
		public void write(BlockIDWritable key, IntArrayWritable value)
				throws IOException, InterruptedException {
			value.write(out);
		}
		
	}
	@Override
	public RecordWriter<BlockIDWritable, IntArrayWritable> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		boolean isCompressed = getCompressOutput(job);
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					job, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
					conf);
			extension = codec.getDefaultExtension();
		}
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(conf);
		if (!isCompressed) {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new BlockRecordWriter(new DataOutputStream(
						new BufferedOutputStream(fileOut, 16 * 1024)));
		} else {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new BlockRecordWriter(new DataOutputStream(new BufferedOutputStream(codec
					.createOutputStream(fileOut), 16 * 1024)));
		}
	}

}

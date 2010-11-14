package zone.io;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import zone.BlockIDWritable;
import zone.PairWritable;

public class StarOutputFormat extends
		FileOutputFormat<BlockIDWritable, PairWritable> {
	protected static class BlockRecordWriter implements RecordWriter<BlockIDWritable, PairWritable> {
		private DataOutputStream out;
		
		public BlockRecordWriter(DataOutputStream out) {
			this.out = out;
		}

		@Override
		public void close(Reporter arg0) throws IOException {
			out.close();
		}

		@Override
		public void write(BlockIDWritable key, PairWritable value)
				throws IOException {
			out.writeDouble(value.getDist());
			out.writeLong(value.get(0).objID);
			out.writeLong(value.get(1).objID);
		}
		
	}

	@Override
	public RecordWriter<BlockIDWritable, PairWritable> getRecordWriter(
			FileSystem ignored, JobConf job, String name, Progressable progress)
			throws IOException {
		boolean isCompressed = getCompressOutput(job);
		if (!isCompressed) {
			Path file = FileOutputFormat.getTaskOutputPath(job, name);
			FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream fileOut = fs.create(file, progress);
			return new BlockRecordWriter(fileOut);
		} else {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					job, GzipCodec.class);
			// create the named codec
			CompressionCodec codec = ReflectionUtils.newInstance(codecClass,
					job);
			// build the filename including the extension
			Path file = FileOutputFormat.getTaskOutputPath(job, name
					+ codec.getDefaultExtension());
			FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream fileOut = fs.create(file, progress);
			return new BlockRecordWriter(new DataOutputStream(codec
					.createOutputStream(fileOut)));
		}
	}
}

package zone.sampling;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import zone.BlockIDWritable;
import zone.NeighborSearch;
import zone.Star;
import zone.io.StarInputFormat;

public class BlockSize {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Star, BlockIDWritable, LongWritable> {
		public Map() {
			NeighborSearch.init();
		}

		public void map(LongWritable key, Star value,
				OutputCollector<BlockIDWritable, LongWritable> output,
				Reporter reporter) throws IOException {
			BlockIDWritable loc = new BlockIDWritable(value.ra, value.dec);
			output.collect(loc, new LongWritable(1));
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<BlockIDWritable, LongWritable, BlockIDWritable, LongWritable> {
		public void reduce(BlockIDWritable key, Iterator<LongWritable> values,
				OutputCollector<BlockIDWritable, LongWritable> output,
				Reporter reporter) throws IOException {
			long num = 0;
			int i = 0;
			while (values.hasNext()) {
				i++;
				LongWritable n = values.next();
				num += n.get();
			}
			output.collect(key, new LongWritable(num));
			System.out.println("block " + key + ", size: " + num);
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(ZoneSize.class);
		conf.setJobName("star searching");

		conf.setOutputKeyClass(BlockIDWritable.class);
		conf.setOutputValueClass(LongWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setNumReduceTasks(1);

		conf.setInputFormat(StarInputFormat.class);
		conf.setOutputFormat(BlockOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}

}

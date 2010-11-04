package zone.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Sort {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		Text key1 = new Text();
		Text value1 = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			String str = value.toString();
			int outVEnd = str.indexOf(')');
			if (outVEnd == -1)
				return;
			int outKBegin = str.indexOf('(', outVEnd);
			key1.set(str.substring(outKBegin));
			value1.set(str.substring(0, outVEnd + 1));
			output.collect(key1, value1);
		}
	}

	public static class Reduce extends MapReduceBase
			implements
			Reducer<Text, Text, Text, Text> {
		Text value = new Text();
		
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			Vector<Text> v = new Vector<Text>();
			while (values.hasNext()) {
				v.add(values.next());
				output.collect(key, value);
			}
			if (v.size() > 1)
				System.out.println(key.toString() + v);
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Sort.class);
		conf.setJobName("star searching");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		// conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}

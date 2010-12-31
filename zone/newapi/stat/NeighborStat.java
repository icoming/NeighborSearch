package zone.newapi.stat;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import zone.BlockIDWritable;
import zone.Star;
import zone.StarZone;
import zone.newapi.MapClass;
import zone.newapi.io.StarInputFormat;

class FinalMapClass extends Mapper<LongWritable, Text, LongWritable, IntArrayWritable> {
	private LongWritable one = new LongWritable(1);
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		System.out.println(line);
		int idx = line.indexOf('\t');
		if (idx == -1)
			return;

		String nums[] = line.substring(idx + 1).split(" ");
		IntArrayWritable arr = new IntArrayWritable(nums.length);
		for (int i = 0; i < nums.length; i++) {
			arr.set(i, new Integer(nums[i]).intValue());
		}
		context.write(one, arr);
	}
}

class FinalReduceClass extends Reducer<LongWritable, IntArrayWritable, LongWritable, IntArrayWritable> { 
	@Override
	public void reduce(LongWritable key, Iterable<IntArrayWritable> values,
			Context context) throws IOException, InterruptedException {
		Iterator<IntArrayWritable> it = values.iterator();
		IntArrayWritable arr = null;
		while (it.hasNext()) {
			IntArrayWritable tmp = it.next();
			if (arr == null)
				arr = tmp.clone();
			else {
				arr.add(tmp);
			}
		}
		context.write(key, arr);
	}
}

public class NeighborStat {
	public static int main(String[] args) throws Exception
	{
		System.out.println("version 1");
		// Get the default configuration object
		Configuration conf = new Configuration();
		
		// Add resources
		conf.addResource("hdfs-default.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-default.xml");
		conf.addResource("mapred-site.xml");	
//		conf.setFloat("mapred.reduce.slowstart.completed.maps", (float) 1.0); 

		Job job = new Job(conf);
		job.setJobName("NeighborStat1");
		
		job.setMapOutputKeyClass(BlockIDWritable.class);
		job.setMapOutputValueClass(StarZone.class);
		
		// the keys are words (strings)
		job.setOutputKeyClass(BlockIDWritable.class);
		// the values are counts (ints)
		job.setOutputValueClass(IntArrayWritable.class);		

		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		//Set the input format class
		job.setInputFormatClass(StarInputFormat.class);
		//Set the output format class
		job.setOutputFormatClass(TextOutputFormat.class);		
		//Set the input path
		StarInputFormat.setInputPaths(job,args[0]);
		//Set the output path
		TextOutputFormat.setOutputPath(job, new Path(args[1] + "-1"));
		
		// Set the jar file to run
		job.setJarByClass(NeighborStat.class);
		
		// Submit the job
		Date startTime = new Date();
		System.out.println("Job started: " + startTime);	
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
				
		if( exitCode == 0) {			
			job = new Job(conf);
			job.setJobName("NeighborStat2");
			
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(IntArrayWritable.class);
			
			// the keys are words (strings)
			job.setOutputKeyClass(LongWritable.class);
			// the values are counts (ints)
			job.setOutputValueClass(IntArrayWritable.class);		

			job.setMapperClass(FinalMapClass.class);
			job.setReducerClass(FinalReduceClass.class);

			job.setNumReduceTasks(1);
			
			//Set the input format class
			job.setInputFormatClass(TextInputFormat.class);
			//Set the output format class
			job.setOutputFormatClass(TextOutputFormat.class);		
//			TextInputFormat.setMinInputSplitSize(job, size);
			//Set the input path
			TextInputFormat.setInputPaths(job,args[1] + "-1");
			//Set the output path
			TextOutputFormat.setOutputPath(job, new Path(args[1] + "-2"));
			
			// Set the jar file to run
			job.setJarByClass(NeighborStat.class);
			exitCode = job.waitForCompletion(true) ? 0 : 1;
		}
		
		if (exitCode == 0) {
			
			Date end_time = new Date();
			System.out.println("Job ended: " + end_time);
			System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");						
		} else {
			System.out.println("Job Failed!!!");
		}
		
		return exitCode;
		
	}

}

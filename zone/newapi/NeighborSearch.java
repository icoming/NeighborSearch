package zone.newapi;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import zone.BlockIDWritable;
import zone.PairWritable;
import zone.StarZone;
import zone.newapi.io.StarInputFormat;
import zone.newapi.io.StarOutputFormat;

public class NeighborSearch {
	public static int main(String[] args) throws Exception
	{
		// Get the default configuration object
		Configuration conf = new Configuration();
		
		// Add resources
		conf.addResource("hdfs-default.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-default.xml");
		conf.addResource("mapred-site.xml");

		Job job = new Job(conf);
		job.setJobName("NeighborSearch");
		
		job.setMapOutputKeyClass(BlockIDWritable.class);
		job.setMapOutputValueClass(StarZone.class);
		
		// the keys are words (strings)
		job.setOutputKeyClass(BlockIDWritable.class);
		// the values are counts (ints)
		job.setOutputValueClass(PairWritable.class);		

		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		//Set the input format class
		job.setInputFormatClass(StarInputFormat.class);
		//Set the output format class
		job.setOutputFormatClass(StarOutputFormat.class);		
		//Set the input path
		StarInputFormat.setInputPaths(job,args[0]);
		//Set the output path
		StarOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Set the jar file to run
		job.setJarByClass(NeighborSearch.class);
		
		// Submit the job
		Date startTime = new Date();
		System.out.println("Job started: " + startTime);	
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
				
		if( exitCode == 0) {			
			Date end_time = new Date();
			System.out.println("Job ended: " + end_time);
			System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");						
		} else {
			System.out.println("Job Failed!!!");
		}
		
		return exitCode;
		
	}

}

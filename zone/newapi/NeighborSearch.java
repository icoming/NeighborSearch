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
	static public final int numZones = 180;
	static public final int numBlocks = 360;
	static public double theta = 1.0/60.0;
	static public double blockWidth = 360.0 / numBlocks;
	static public double zoneHeight = 180.0 / numZones;
	static public double blockRanges[][] = new double[numBlocks][2];
	static public double zoneRanges[][] = new double[numZones][2];
	static public double maxAlphas[] = new double[numZones];
	static public double costheta = Math.cos(Math.toRadians(theta));

	static public double calAlpha(double theta, double dec) {
		if (Math.abs(dec) + theta > 89.9)
			return 180;

		return (double) Math.toDegrees(Math.abs(Math.atan(Math.sin(Math
				.toRadians(theta))
				/ Math.sqrt(Math.cos(Math.toRadians(dec - theta))
						* Math.cos(Math.toRadians(dec + theta))))));
	}
	
	public static void init() {
		zoneRanges[0][0] = -90;
		zoneRanges[0][1] = -90 + zoneHeight;
		for (int i = 1; i < zoneRanges.length; i++) {
			zoneRanges[i][0] = zoneRanges[i - 1][1];
			zoneRanges[i][1] = zoneRanges[i][0] + zoneHeight;
		}

		blockRanges[0][0] = 0;
		blockRanges[0][1] = blockWidth;
		for (int i = 1; i < blockRanges.length; i++) {
			blockRanges[i][0] = blockRanges[i - 1][1];
			blockRanges[i][1] = blockRanges[i][0] + blockWidth;
		}

		for (int i = 0; i < maxAlphas.length; i++) {
			double maxDec = zoneRanges[i][1];
			if (maxDec <= 0)
				maxDec = zoneRanges[i][0];
			maxAlphas[i] = calAlpha(theta, maxDec);
		}
	}

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

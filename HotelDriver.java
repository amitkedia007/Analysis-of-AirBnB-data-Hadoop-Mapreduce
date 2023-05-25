package org.myorg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;


public class HotelDriver
{

	public static void main(String[] args) throws Exception
	{
		// Create a new Hadoop Configuration object
		Configuration conf = new Configuration();
		// Check if the correct number of arguments has been passed
		if (args.length != 5)
		{
			System.err.println("Usage: HotelDriver <input path> <output path>");
			System.exit(-1);
		}
		
		// Get the FileSystem object and delete the directory if exists
		FileSystem hdfs = FileSystem.get(conf);
		Path outputDir = new Path(args[2]);
		if (hdfs.exists(outputDir))
  	    hdfs.delete(outputDir, true);
		
		 // Define the first MapReduce job
		Job job1;
		job1=Job.getInstance(conf, "Avg score");
		job1.setJarByClass(HotelDriver.class);
		
		// Set input and output paths for the first job
		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));

		// Set mapper and reducer classes for the first job
		job1.setMapperClass(HotelMapper1.class);
		job1.setReducerClass(HotelReducer1.class);

		// Set output key and value classes for the first job
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		
		// Wait for the first job to complete
		job1.waitForCompletion(true);
		
		// Define the second MapReduce job
		Job job2;
		job2=Job.getInstance(conf, "Avg score");
		job2.setJarByClass(HotelDriver.class);
		
		// Set input and output paths for the second job
		FileInputFormat.addInputPath(job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		
		 // Set mapper and reducer classes for the second job
		job2.setMapperClass(HotelMapper2.class);
		job2.setReducerClass(HotelReducer2.class);
		
		// Set output key and value classes for the second job
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		// Wait for the second job to complete
		job2.waitForCompletion(true);
		
		// Define the third MapReduce job
		Job job3;
		job3=Job.getInstance(conf, "Min Max Score");
		job3.setJarByClass(HotelDriver.class);
		
		// Set input and output paths for the third job
		FileInputFormat.addInputPath(job3, new Path(args[3]));
		FileOutputFormat.setOutputPath(job3, new Path(args[4]));
		
		// Set mapper and reducer classes for the third job
		job3.setMapperClass(HotelMapper3.class);
		job3.setReducerClass(HotelReducer3.class);
		job3.setMapOutputKeyClass(Text.class);
	    job3.setMapOutputValueClass(NullWritable.class);
	    
	    // Set output key and value classes for the third job
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
      		
		// Wait for the third job to complete
		System.exit(job3.waitForCompletion(true) ? 0 : 1);								
	}
}



 
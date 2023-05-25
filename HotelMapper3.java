package org.myorg;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
 


public class HotelMapper3 extends Mapper<LongWritable, Text, Text, NullWritable> {
 
    @Override
    
   // the input for the Mapper3 comes from the output of the reducer2
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	 // Write each input line as a Text key with a NullWritable value
        // This is done to ensure that all records pass through to the reducer
        context.write(value, NullWritable.get());
    }
}

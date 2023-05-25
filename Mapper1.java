package org.myorg;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Define the class HotelMapper1 that extends the Mapper class
public class HotelMapper1 extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	// Define a DoubleWritable object to hold the score for each line of input
	private final static DoubleWritable tempDoubleWritable = new DoubleWritable();
	// Define a Text object to hold the concatenation of the city and room type for each line of input
	private Text City_roomtype = new Text();

	//Override the Mapper class's map function to read and process input records
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
			{
		// Split the input line into an array of values using comma as the delimiter
				String[] line = value.toString().split(",");
				// Concatenate the city and room type fields and set it as the key for this record
				City_roomtype = new Text(line[20] + ',' + line[2]);
				
				// Check if the score field is a valid numeric value because the first line is text
				if (line[10].matches("-?\\d+(\\.\\d+)?")) {
					// Convert the score value to a double and set it as the value for this record
					double score = Double.parseDouble(line[10].trim());
					tempDoubleWritable.set(score);
					// Emit the key-value pair to the output collector
					context.write(City_roomtype, tempDoubleWritable);
				} else {
					// Handle the case where the value is not numeric
					System.err.println("Invalid score: " + line[10]);
				}
				
				// Emit the key-value pair to the output collector
				context.write(City_roomtype, tempDoubleWritable);
			}
}


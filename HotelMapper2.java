package org.myorg;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Defining the class for the second mapper which takes input from the output of Reducer1
public class HotelMapper2 extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	// Creating a DoubleWritable object to hold the score
	private final static DoubleWritable tempDoubleWritable = new DoubleWritable();
	// Creating a Text object to hold the city name
	private Text City = new Text();

	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
			{
		        // Split the input value by tab character as key and value are seperated by tab
				String[] line = value.toString().split("\t");
				
				// Split the first field (which is city and room type) by comma character
				String[] temp = line[0].toString().split(",");
				
				// Setting the city name as Text object
				City = new Text(temp[0]);
				// To check if this is the first line (header) and skip it
				 if (key.get() == 0) { 
					 return;
			        }
				// Get the average score from the second field of the input
				double avg_score = Double.parseDouble(line[1].trim());
				// Set the score as DoubleWritable object
				tempDoubleWritable.set(avg_score);
				// Emit the key-value pair (city name, score) to the reducer
				context.write(City, tempDoubleWritable);
			}
}



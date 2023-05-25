package org.myorg;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Defining the class HotelReducer1 that extends the Reducer class
public class HotelReducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
	// Define an ArrayList to store the scores
	ArrayList<Double> avg_score = new ArrayList<Double>();
	
	// Override the Reducer class's reduce function to process input key-value pairs and generate output
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException
	{
		// Initialize a variable to keep track of the sum of scores
		double sumofscore = 0.0;
		
		// Iterate over all the scores and add them to the ArrayList
		for (DoubleWritable value: values)
		{
			avg_score.add(value.get());
			sumofscore = sumofscore + value.get();
		}
		// Get the number of scores in the ArrayList
		int size = avg_score.size();
		
		// Clear the ArrayList for the next key-value pair
		avg_score.clear();
		
		// Calculate the average score for the key-value pair
		double avg = sumofscore / size;
		// Write the output in the hadoop where value is DoubleWritable
		context.write(key, new DoubleWritable(avg));
	}
}

package org.myorg;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class HotelReducer2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
	ArrayList<Double> final_avg_score = new ArrayList<Double>();
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException
	{
		double sumofscore = 0.0;
		
		// Loop through all the values in the Iterable
				for (DoubleWritable value: values)
				{
					// Add each value to the final_avg_score ArrayList
					final_avg_score.add(value.get());
					
					// Calculate the sum of all the scores for the current key
					sumofscore = sumofscore + value.get();
				}
				
				// Get the number of scores for the current key
				int size = final_avg_score.size();
				
				// Clear the ArrayList to avoid memory leaks
				final_avg_score.clear();
				
				// Calculate the average score for the current key
				double avg = sumofscore / size;
				
				// Write the key-value pair to the context
				context.write(key, new DoubleWritable(avg));
			}
		}


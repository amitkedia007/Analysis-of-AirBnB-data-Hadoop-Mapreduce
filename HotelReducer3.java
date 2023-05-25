package org.myorg;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

//The reducer takes input in the form of Text and NullWritable and outputs Text and Text.
public class HotelReducer3 extends Reducer<Text, NullWritable, Text, Text> {
	
	// Creating four private instances to store city names and satisfaction scores
    private Text highestCity = new Text();
    private Text lowestCity = new Text();
    private DoubleWritable highestScore = new DoubleWritable(Double.MIN_VALUE);
    private DoubleWritable lowestScore = new DoubleWritable(Double.MAX_VALUE);

    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) 
            throws IOException, InterruptedException {
    	
    	// Splitting the key value pair that is received from the mapper 
        
    	String line = key.toString();
        String[] tokens = line.split("\t");

        // Extract city name and satisfaction score
        // Storing the first city and score into the static variable
        String city = tokens[0];
        double score = Double.parseDouble(tokens[1]);

        // Will compare the static variables with the next value
        // Update highest and lowest scores if necessary
        if (score > highestScore.get()) {
            highestScore.set(score);
            highestCity.set(city);
        }
        if (score < lowestScore.get()) {
            lowestScore.set(score);
            lowestCity.set(city);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Output the final result
        context.write(new Text("City with highest satisfaction score: "), new Text(highestCity + " " + highestScore));
        context.write(new Text("City with lowest satisfaction score: "), new Text(lowestCity + " " + lowestScore));
    }
}


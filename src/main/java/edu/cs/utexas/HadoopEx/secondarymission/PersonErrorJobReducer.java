package edu.cs.utexas.HadoopEx.secondarymission;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PersonErrorJobReducer extends  Reducer<Text, IntWritable, Text, FloatWritable> {

   public void reduce(Text key, Iterable<IntWritable> values, Context context)
           throws IOException, InterruptedException {
        
        int counter = 0;
        int sum = 0;
       
       for (IntWritable value : values) {
           counter++;
           sum += value.get();
       }
       
       float errorFraction = ((float) sum / counter);


       context.write(key, new FloatWritable(errorFraction));
   }
}
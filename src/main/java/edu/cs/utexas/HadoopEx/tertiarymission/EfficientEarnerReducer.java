package edu.cs.utexas.HadoopEx.tertiarymission;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EfficientEarnerReducer extends  Reducer<Text, IntFloatWritable, Text, FloatWritable> {

   public void reduce(Text key, Iterable<IntFloatWritable> values, Context context)
           throws IOException, InterruptedException {
        
       int tripTime = 0;
       float money = 0;

       for (IntFloatWritable value : values) {
           tripTime += value.getIntWritable().get();
           money += value.getFloatWritable().get();
       }

       
       float dollarPerTime = money / tripTime;

       context.write(new Text(key), new FloatWritable(dollarPerTime));
   }
}
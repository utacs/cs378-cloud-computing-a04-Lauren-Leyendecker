package edu.cs.utexas.HadoopEx.tertiarymission;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EfficientEarnerReducer extends  Reducer<Text, TupleWritable, Text, FloatWritable> {

   public void reduce(Text key, Iterable<TupleWritable> values, Context context)
           throws IOException, InterruptedException {
        
       int tripTime = 0;
       float money = 0;

       for (TupleWritable value : values) {
           tripTime += ((IntWritable) value.get(0)).get();
           money += ((FloatWritable) value.get(1)).get();
       }
       
       float dollarPerTime = money / tripTime;

       context.write(new Text(key), new FloatWritable(dollarPerTime));
   }
}
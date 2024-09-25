package edu.cs.utexas.HadoopEx.tertiarymission;

import edu.cs.utexas.HadoopEx.secondarymission.TaxiRecord;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;


public class TopKJobReducer extends  Reducer<Text, FloatWritable, Text, FloatWritable> {

    private PriorityQueue<TaxiMoneyRecord> pq = new PriorityQueue<>(5);
    private Logger logger = Logger.getLogger(TopKJobReducer.class);
    
    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
   public void reduce(Text key, Iterable<FloatWritable> values, Context context)
           throws IOException, InterruptedException {

        int counter = 0;

       // size of values is 1 because key only has one distinct value
       for (FloatWritable value : values) {
           counter++;
           logger.info("Reducer Text: counter is " + counter);
           logger.info("Reducer Text: Add this item  " + new TaxiRecord(key, value).toString());

           pq.add(new TaxiMoneyRecord(new Text(key), new FloatWritable(value.get())));

           logger.info("Reducer Text: " + key.toString() + " , Count: " + value.toString());
           logger.info("PQ Status: " + pq.toString());
       }

       // keep the priorityQueue size <= heapSize
       while (pq.size() > 5) {
           pq.poll();
       }
   }


    public void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("TopKReducer cleanup cleanup.");
        logger.info("pq.size() is " + pq.size());

        List<TaxiMoneyRecord> values = new ArrayList<>(5);

        while (!pq.isEmpty()) {
            values.add(pq.poll());
        }

        logger.info("values.size() is " + values.size());
        logger.info(values.toString());

        // reverse so they are ordered in descending order
        Collections.reverse(values);

        for (TaxiMoneyRecord value : values) {
            context.write(value.getHack(), value.getMoneyPerTime());
            logger.info("TopKReducer - Top-10 Words are:  " + value.getHack() + "  Count:" + value.getMoneyPerTime());
        }
    }
}
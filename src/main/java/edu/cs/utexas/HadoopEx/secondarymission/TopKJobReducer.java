package edu.cs.utexas.HadoopEx.secondarymission;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Iterator;



public class TopKJobReducer extends  Reducer<Text, FloatWritable, Text, FloatWritable> {

    private PriorityQueue<TaxiRecord> pq = new PriorityQueue<TaxiRecord>(5);
    private Logger logger = Logger.getLogger(TopKJobReducer.class);
    
    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * @param text
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

           pq.add(new TaxiRecord(key, value));

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

        List<TaxiRecord> values = new ArrayList<TaxiRecord>(5);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        logger.info("values.size() is " + values.size());
        logger.info(values.toString());

        // reverse so they are ordered in descending order
        Collections.reverse(values);

        for (TaxiRecord value : values) {
            context.write(value.getMedallion(), value.getErrorFraction());
            logger.info("TopKReducer - Top-10 Words are:  " + value.getMedallion() + "  Count:" + value.getErrorFraction());
        }
    }
}
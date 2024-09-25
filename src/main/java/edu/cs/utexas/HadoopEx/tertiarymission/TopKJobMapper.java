package edu.cs.utexas.HadoopEx.tertiarymission;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.PriorityQueue;


public class TopKJobMapper extends Mapper<Text, Text, Text, FloatWritable> {

	private Logger logger = Logger.getLogger(TopKJobMapper.class);


	private PriorityQueue<TaxiMoneyRecord> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>();

	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		pq.add(new TaxiMoneyRecord(new Text(key), new FloatWritable(Float.parseFloat(value.toString()))));
		logger.info("TopKMapper Key Status: " + key.toString());
		if (pq.size() > 5) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {

		while (!pq.isEmpty()) {
			TaxiMoneyRecord record = pq.poll();
			context.write(record.getHack(), record.getMoneyPerTime());
			logger.info("TopKMapper PQ Status: " + pq.toString());
		}
	}

}
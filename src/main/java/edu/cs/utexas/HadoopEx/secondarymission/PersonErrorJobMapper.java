package edu.cs.utexas.HadoopEx.secondarymission;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import lombok.SneakyThrows;

public class PersonErrorJobMapper extends Mapper<Object, Text, Text, IntWritable> {

	@SneakyThrows
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String[] splitData = value.toString().split(",");

		// need 16 commas
		if(splitData.length != 17){
			return;
		}

        int errors = anyErrors(splitData[6], splitData[7]) + anyErrors(splitData[8], splitData[9]);

        context.write(new Text(splitData[0]), new IntWritable(errors));
	}

	@SneakyThrows
	public int anyErrors(String s_long, String s_lat){

		try{
			float longt = Float.parseFloat(s_long);
			float lat = Float.parseFloat(s_lat);

			if((longt == 0) || (lat == 0)){
				return 1;
			} 
		} catch (NumberFormatException e) {
			return 1;
		}

        return 0;
	}

}
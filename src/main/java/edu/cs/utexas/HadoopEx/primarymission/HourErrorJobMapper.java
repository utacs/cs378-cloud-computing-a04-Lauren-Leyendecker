package edu.cs.utexas.HadoopEx.primarymission;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import lombok.SneakyThrows;

public class HourErrorJobMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    @SneakyThrows
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] splitData = value.toString().split(",");

        // need 16 commas
        if (splitData.length != 17) {
            return;
        }

        anyErrors(splitData[2], splitData[6], splitData[7], context);
        anyErrors(splitData[3], splitData[8], splitData[9], context);

    }

    @SneakyThrows
    public void anyErrors(String s_time, String s_long, String s_lat, Context context) {
        if (s_time.length() == 0) {
            return;
        }

        int hour;
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime dateTime = LocalDateTime.parse(s_time, formatter);
            hour = dateTime.getHour();
        } catch (DateTimeParseException e) {
            return;
        }
        hour++;

        try {
            float longt = Float.parseFloat(s_long);
            float lat = Float.parseFloat(s_lat);

            if ((longt == 0) || (lat == 0)) {
                context.write(new IntWritable(hour), new IntWritable(1));
            }
        } catch (NumberFormatException e) {
            context.write(new IntWritable(hour), new IntWritable(1));
            return;
        }
    }

}
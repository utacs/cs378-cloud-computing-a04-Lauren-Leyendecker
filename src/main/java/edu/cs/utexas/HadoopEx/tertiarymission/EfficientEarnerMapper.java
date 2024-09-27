package edu.cs.utexas.HadoopEx.tertiarymission;

import lombok.SneakyThrows;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EfficientEarnerMapper extends Mapper<Object, Text, Text, IntFloatWritable> {

    @SneakyThrows
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] splitData = value.toString().split(",");

        // need 16 commas
        if (splitData.length != 17) {
            return;
        }

        String hackLicense = splitData[1];

        int timeOfTrip;
        try {
            timeOfTrip = Integer.parseInt(splitData[4]);
        } catch (NumberFormatException e) {
            return;
        }

        if(timeOfTrip == 0){
            return;
        }

        float floatSum = 0;
        try {
            for (int i = 11; i <= 15; i++) {
                String currentFloat = splitData[i];
                floatSum += Float.parseFloat(currentFloat);

            }
            float totalAmount = Float.parseFloat(splitData[16]);

            if (Math.abs(totalAmount - floatSum) > 0.0001) {
                return;
            }

        } catch (NumberFormatException e) {
            return;
        }

        context.write(new Text(hackLicense), new IntFloatWritable(timeOfTrip, floatSum));

    }

}
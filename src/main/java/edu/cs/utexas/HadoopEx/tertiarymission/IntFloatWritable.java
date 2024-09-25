package edu.cs.utexas.HadoopEx.tertiarymission;

import lombok.Getter;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
public class IntFloatWritable implements Writable {

    private IntWritable intWritable;
    private FloatWritable floatWritable;

    public IntFloatWritable() {
        this.intWritable = new IntWritable();
        this.floatWritable = new FloatWritable();
    }

    public IntFloatWritable(int intValue, float floatValue) {
        this.intWritable = new IntWritable(intValue);
        this.floatWritable = new FloatWritable(floatValue);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        intWritable.write(out);
        floatWritable.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        intWritable.readFields(in);
        floatWritable.readFields(in);
    }
}

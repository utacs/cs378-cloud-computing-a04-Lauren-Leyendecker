package edu.cs.utexas.HadoopEx.secondarymission;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import lombok.Getter;

@Getter
public class TaxiRecord implements Comparable<TaxiRecord> {

        private final Text medallion;
        private final FloatWritable errorFraction;

        public TaxiRecord(Text medallion, FloatWritable errorFraction) {
            this.medallion = medallion;
            this.errorFraction = errorFraction;
        }

    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
        @Override
        public int compareTo(TaxiRecord other) {
            return errorFraction.compareTo(other.errorFraction);
        }


        public String toString(){
            return "(" + medallion.toString() + " , " + errorFraction.toString() + ")";
        }
    }


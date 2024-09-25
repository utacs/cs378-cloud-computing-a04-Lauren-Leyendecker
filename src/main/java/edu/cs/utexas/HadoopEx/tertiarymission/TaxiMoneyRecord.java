package edu.cs.utexas.HadoopEx.tertiarymission;

import lombok.Getter;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

@Getter
public class TaxiMoneyRecord implements Comparable<TaxiMoneyRecord> {

    private final Text hack;
    private final FloatWritable moneyPerTime;

    public TaxiMoneyRecord(Text medallion, FloatWritable moneyPerTime) {
        this.hack = medallion;
        this.moneyPerTime = moneyPerTime;
    }

    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
    @Override
    public int compareTo(TaxiMoneyRecord other) {
        return moneyPerTime.compareTo(other.moneyPerTime);
    }


    public String toString(){
        return "(" + hack.toString() + " , " + moneyPerTime.toString() + ")";
    }
}


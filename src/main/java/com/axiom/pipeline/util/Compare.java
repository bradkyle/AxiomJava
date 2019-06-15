package com.axiom.pipeline.util;


import java.util.Comparator;
import org.apache.beam.sdk.values.Row;
import java.util.Map.Entry;

public class Compare {
    public static class TimeComparator implements Comparator<Row>{
        @Override
        public int compare(Row o1, Row o2) {
            Long a = o1.getInt64("time");
            Long b = o2.getInt64("time");
            return a.compareTo(b);
        }
    }

    public static class LevelComparator implements Comparator<Entry<Double,Double>>{

        final String side;

        public LevelComparator(
            String side
        ) {
            this.side=side;
        }

        @Override
        public int compare(Entry<Double,Double> o1, Entry<Double,Double> o2) {
            Double a = o1.getKey();
            Double b = o2.getKey();
            if (this.side.equals("ask")){
                if (a < b) 
                    return -1;
                else if (a == b)
                    return 0;
                else
                    return 1;
            } else {
                if (a < b) 
                    return 1;
                else if (a == b)
                    return 0;
                else
                    return -1;
            }
            
        }
    }
}
package com.epam.biggdata.minskq3.task4.shuffeling;

import com.epam.biggdata.minskq3.task4.model.KeyIdTime;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyIdTimeGroupingComparator extends WritableComparator{
    public KeyIdTimeGroupingComparator() {
        super(KeyIdTime.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        KeyIdTime o1 = (KeyIdTime)a;
        KeyIdTime o2 = (KeyIdTime)b;
        return o1.getiPinionID().compareToIgnoreCase(o2.getiPinionID());
    }
}

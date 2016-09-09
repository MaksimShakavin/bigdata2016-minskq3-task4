package com.epam.biggdata.minskq3.task4.shuffeling;


import com.epam.biggdata.minskq3.task4.model.KeyIdTime;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyIdTimeSortComparator extends WritableComparator{
    public KeyIdTimeSortComparator() {
        super(KeyIdTime.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return a.compareTo(b);
    }
}

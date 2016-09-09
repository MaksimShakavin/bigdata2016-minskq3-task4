package com.epam.biggdata.minskq3.task4.shuffeling;

import com.epam.biggdata.minskq3.task4.model.KeyIdTime;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyIdPartitioner extends Partitioner<KeyIdTime, NullWritable>{
    @Override
    public int getPartition(KeyIdTime keyIdTime, NullWritable nullWritable, int numPartitions) {
        return keyIdTime.getiPinionID().hashCode() % numPartitions;
    }
}

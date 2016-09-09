package com.epam.biggdata.minskq3.task4;


import com.epam.biggdata.minskq3.task4.model.KeyIdTime;
import com.epam.biggdata.minskq3.task4.shuffeling.KeyIdPartitioner;
import com.epam.biggdata.minskq3.task4.shuffeling.KeyIdTimeGroupingComparator;
import com.epam.biggdata.minskq3.task4.shuffeling.KeyIdTimeSortComparator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Optional;
import java.util.stream.StreamSupport;

public class Main {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();

        job.setJarByClass(Main.class);
        job.setJobName(Main.class.getSimpleName());

        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);

        job.setMapOutputKeyClass(KeyIdTime.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(KeyIdPartitioner.class);
        job.setSortComparatorClass(KeyIdTimeSortComparator.class);
        job.setGroupingComparatorClass(KeyIdTimeGroupingComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.out.println("Job completed with code: " + result);

        Counters counters = job.getCounters();
        Optional<Counter> maxValueCounter = StreamSupport.stream(counters.getGroup("MaxImpressions").spliterator(),false)
                .max((c1,c2)-> Long.compare(c1.getValue(),c2.getValue()));

        maxValueCounter.ifPresent(counter -> {
            System.out.println("Max site impression:");
            System.out.println("IPinId:"+ counter.getDisplayName() + " impression: "+ counter.getValue());
        });

    }
}

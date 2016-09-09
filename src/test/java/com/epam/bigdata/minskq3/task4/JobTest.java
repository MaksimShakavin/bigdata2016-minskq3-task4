package com.epam.bigdata.minskq3.task4;

import com.epam.biggdata.minskq3.task4.JobMapper;
import com.epam.biggdata.minskq3.task4.JobReducer;
import com.epam.biggdata.minskq3.task4.model.KeyIdTime;
import com.epam.biggdata.minskq3.task4.shuffeling.KeyIdTimeGroupingComparator;
import com.epam.biggdata.minskq3.task4.shuffeling.KeyIdTimeSortComparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JobTest {

    MapDriver<Object, Text, KeyIdTime, Text> mapDriver;
    ReduceDriver<KeyIdTime, Text, NullWritable, Text> reduceDriver;
    ReduceDriver<KeyIdTime, Text, NullWritable, Text> reduce2Driver;

    MapReduceDriver<Object, Text, KeyIdTime, Text, NullWritable, Text> mapReduceDriver;

    private String[] input = new String[]{
            "8c66f1538798b7ab57e2da7be11c5696	20130606222224943	Z0KpO7S8PQpNDBa	Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)	192.168.0.*	...	300	250	0	0	100	e1af08818a6cd6bbba118bb54a651961    150	3476	282825712806	1",
            "71e9a371b25950af4e28688743260f3b   20130606001907400   VhkSLxSELTuOkGn Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0) 192.168.1.* ... 300 250 0   0   100 e1af08818a6cd6bbba118bb54a651961    150 3476    282825712806    1",
            "71e9a371b25950af4e28688743260f3b   20130606001907401   VhkSLxSELTuOkGn Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0) 192.168.1.* ... 300 250 0   0   100 e1af08818a6cd6bbba118bb54a651961    150 3476    282825712806    1",
            "8c66f1538798b7ab57e2da7be11c5696	20130606222224944	Z0KpO7S8PQpNDBa	Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)	192.168.0.*	...	300	250	0	0	100	e1af08818a6cd6bbba118bb54a651961    150	3476	282825712806	0",

    };

    private String[] ids = new String[]{
            "VhkSLxSELTuOkGn",
            "Z0KpO7S8PQpNDBa"
    };

    private long[] timestamps = new long[]{
            20130606222224943L,
            20130606001907400L,
            20130606001907401L,
            20130606222224944L
    };



    @Before
    public void setUp(){
        JobMapper mapper = new JobMapper();
        JobReducer reducer = new JobReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.setKeyGroupingComparator(new KeyIdTimeGroupingComparator());
        mapReduceDriver.setKeyOrderComparator(new KeyIdTimeSortComparator());
    }

    @Test
    public void testMapper() throws IOException{
        Arrays.stream(input).forEach(line ->
                mapDriver.withInput(new LongWritable(), new Text(line)));

        mapDriver.withOutput(new KeyIdTime(ids[1],timestamps[0]),new Text(input[0]));
        mapDriver.withOutput(new KeyIdTime(ids[0],timestamps[1]),new Text(input[1]));
        mapDriver.withOutput(new KeyIdTime(ids[0],timestamps[2]),new Text(input[2]));
        mapDriver.withOutput(new KeyIdTime(ids[1],timestamps[3]),new Text(input[3]));

        mapDriver.runTest();
    }

    @Test
    public void testResucer() throws Exception{
        List<Text> values1 = new ArrayList<>();
        values1.add(new Text(input[0]));
        List<Text> values4 = new ArrayList<>();
        values4.add(new Text(input[3]));
        reduceDriver.withInput(new KeyIdTime(ids[1], timestamps[0]), values1);
        reduceDriver.withInput(new KeyIdTime(ids[1], timestamps[3]), values4);

        List<Text> values2 = new ArrayList<>();
        values2.add(new Text(input[1]));
        List<Text> values3 = new ArrayList<>();
        values3.add(new Text(input[2]));
        reduceDriver.withInput(new KeyIdTime(ids[0], timestamps[1]), values2);
        reduceDriver.withInput(new KeyIdTime(ids[0], timestamps[2]), values3);

        reduceDriver.withOutput(NullWritable.get(), new Text(input[0]));
        reduceDriver.withOutput(NullWritable.get(), new Text(input[3]));
        reduceDriver.withOutput(NullWritable.get(), new Text(input[1]));
        reduceDriver.withOutput(NullWritable.get(), new Text(input[2]));

        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws Exception{
        Arrays.stream(input).forEach(line ->
                mapReduceDriver.withInput(new LongWritable(), new Text(line)));

        mapReduceDriver.withOutput(NullWritable.get(), new Text(input[1]));
        mapReduceDriver.withOutput(NullWritable.get(), new Text(input[2]));
        mapReduceDriver.withOutput(NullWritable.get(), new Text(input[0]));
        mapReduceDriver.withOutput(NullWritable.get(), new Text(input[3]));

        mapReduceDriver.runTest();

        long maxVal = mapReduceDriver.getCounters().findCounter("MaxImpressions","VhkSLxSELTuOkGn").getValue();
        assertEquals(2,maxVal);
    }
}

package com.epam.biggdata.minskq3.task4;


import com.epam.biggdata.minskq3.task4.model.KeyIdTime;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JobMapper extends Mapper<Object,Text,KeyIdTime,Text> {
    private Text line = new Text();
    private KeyIdTime cikw = new KeyIdTime();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] params = line.split("\\s+");

        this.line.set(line);
        cikw.setiPinionID(params[2]);
        cikw.setTimestamp(Long.parseLong(params[1]));

        context.write(cikw, this.line);
    }
}

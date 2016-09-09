package com.epam.biggdata.minskq3.task4;


import com.epam.biggdata.minskq3.task4.model.KeyIdTime;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JobReducer extends Reducer<KeyIdTime, Text, NullWritable, Text>{
    NullWritable nullKey = NullWritable.get();
    long maxSiteImpressionSum = 0;
    String maxImpressionId;

    public void reduce(KeyIdTime key,
                       Iterable<Text> values,
                       Reducer<KeyIdTime, Text, NullWritable, Text>.Context context
    ) throws IOException, InterruptedException {

        long siteImpressionSum = 0;
        for (Text val : values) {
            context.write(nullKey, val);

            String logLine = val.toString();
            int streamId = Integer.parseInt(logLine.substring(logLine.length() - 1));
            if (streamId == 1) {
                siteImpressionSum++;
            }
        }

        if (maxSiteImpressionSum <= siteImpressionSum) {
            maxSiteImpressionSum = siteImpressionSum;
            maxImpressionId = key.getiPinionID();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.getCounter("MaxImpressions", maxImpressionId).setValue(maxSiteImpressionSum);
    }
}

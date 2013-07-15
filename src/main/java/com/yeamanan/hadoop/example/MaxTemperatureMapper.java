package com.yeamanan.hadoop.example;

import java.io.IOException;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    protected void map(
            final LongWritable argKey,
            final Text argValue,
            final Context argContext)
            throws IOException, InterruptedException {

        if (argKey.get() != 0) {
            String str[] = argValue.toString().split(",");
            if (str.length >= 12) {
                if (!str[12].matches("M")) {
                    float value = Float.parseFloat(str[12]);
                    argContext.write(new Text(str[0] + ";" + str[1]), new FloatWritable(value));
                }
            }
        }

    }
}

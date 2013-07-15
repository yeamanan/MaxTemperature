package com.yeamanan.hadoop.example;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	@Override
	protected void reduce(
			Text argKey,
			Iterable<FloatWritable> argValues,
			Context argContext)
			throws IOException, InterruptedException {

		float maxValue = Float.MIN_VALUE;
		for (FloatWritable value : argValues) {
			maxValue = Math.max(maxValue, value.get());
		}
		argContext.write(argKey, new FloatWritable(maxValue));

	}

}

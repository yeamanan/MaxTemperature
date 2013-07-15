package com.yeamanan.hadoop.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * MaxTemperature class.
 * @author Yeam Anan (<yeamanan@gmail.com>)
 */
public class MaxTemperature {

    /**
     * Logger.
     */
    private static final Logger LOGGER = Logger.getLogger(MaxTemperature.class);

    /**
     * main() method.
     * @param args arguments
     */
    public static void main(final String[] args) {

        for(int i = 0; i < args.length; i++) {
            LOGGER.info(args[i]);
        }

        final MaxTemperatureCommandLine cmdLine =
                new MaxTemperatureCommandLine(args);
        final String inputPath = cmdLine.getInputPath();
        final String outputPath = cmdLine.getOutputPath();

        try {
            LOGGER.info("Start of Job");

            final long start = System.currentTimeMillis();

            FileSystem.get(new Configuration())
                    .delete(new Path(outputPath), true);
            final Job job = new Job();
            job.setJarByClass(MaxTemperature.class);
            job.setJobName("Max Temperature");

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setMapperClass(MaxTemperatureMapper.class);
            if (cmdLine.hasCombinerOption()) {
                job.setCombinerClass(MaxTemperatureReducer.class);
            }
            job.setReducerClass(MaxTemperatureReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);

            final boolean retValue = job.waitForCompletion(true);

            LOGGER.info("End of Job");

            final long end = System.currentTimeMillis();
            LOGGER.info("Time spend for running the job : " + (end - start)
                    + " ms");

            System.exit(retValue ? 0 : 1);
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
                LOGGER.fatal("Fatal error", e);
        }
    }

}

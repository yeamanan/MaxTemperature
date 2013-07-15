package com.yeamanan.hadoop.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.TreeMap;
import org.apache.log4j.Logger;

/**
 * MaxTemperatureLegacy class.
 * @author Yeam Anan (<yeamanan@gmail.com>)
 */
public class MaxTemperatureLegacy {

    /**
     * Logger.
     */
    private static final Logger LOGGER = Logger.getLogger(MaxTemperatureLegacy.class);

    /**
     * main() method.
     * @param args arguments
     */
    public static void main(final String[] args) {

        for (int i = 0; i < args.length; i++) {
            LOGGER.info(args[i]);
        }

        final MaxTemperatureCommandLine cmdLine =
                new MaxTemperatureCommandLine(args);
        final String inputPath = cmdLine.getInputPath();
        final String outputPath = cmdLine.getOutputPath();

        LOGGER.info("Start of Job");

        final long start = System.currentTimeMillis();

        final File directory = new File(inputPath);
        if (directory.isDirectory()) {
            final File[] files = directory.listFiles();
            doWork(files, outputPath);
        } else {
            final File[] files = {directory};
            doWork(files, outputPath);
        }

        LOGGER.info("End of Job");

        final long end = System.currentTimeMillis();
        LOGGER.info("Time spend for running the job : " + (end - start)
                + " ms");
    }

    /**
     * doWork() method.
     * @param files list of input file
     * @param outputFolder output folder
     */
    public static void doWork(final File[] files, final String outputFolder) {
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        final TreeMap<String, Float> map = new TreeMap<String, Float>();
        try {
            fos = new FileOutputStream(new File(outputFolder + "part-r-00000"));
            osw = new OutputStreamWriter(fos);
            for (int i = 0; i < files.length; i++) {
                try {
                    fis = new FileInputStream(files[i]);
                    isr = new InputStreamReader(fis);
                    br = new BufferedReader(isr);
                    String line;
                    String key = "";
                    float value = Float.MIN_VALUE;
                    while ((line = br.readLine()) != null) {
                        String str[] = line.split(",");
                        if (!str[0].matches("WBAN")) {
                            if (str.length >= 12) {
                                if (!str[12].matches("M")) {
                                    String newKey = str[0] + ";" + str[1];
                                    float newValue = Float.parseFloat(str[12]);
                                    if (key.matches(newKey)) {
                                        value = Math.max(value, newValue);
                                    } else {
                                        if (!key.matches("")) {
                                            map.put(key, new Float(value));
                                        }
                                        key = newKey;
                                        value = Float.MIN_VALUE;
                                    }
                                }
                            }
                        }
                    }
                    map.put(key, value);
                } catch (IOException e) {
                    LOGGER.error("Error", e);
                } finally {
                    try {
                        br.close();
                        isr.close();
                        fis.close();
                    } catch (IOException e) {
                        LOGGER.error("Error", e);
                    }
                }
            }
            for (Object entry : map.entrySet()) {
                String k = ((Map.Entry<String, Float>) entry).getKey();
                Float v = ((Map.Entry<String, Float>) entry).getValue();
                osw.write(k + "\t" + v + "\n");
            }
        } catch (IOException e) {
            LOGGER.error("Error", e);
        } finally {
            try {
                osw.close();
                fos.close();
            } catch (IOException e) {
                LOGGER.fatal("Fatal error", e);
            }
        }
    }
}

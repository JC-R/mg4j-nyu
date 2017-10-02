package edu.nyu.tandon.tool.renumber;

import com.martiansoftware.jsap.*;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

/**
 * @author michal.siedlaczek@nyu.edu
 *
 * Convert numbers from text file to a binary encoded list of integers.
 */
public class Lines2Bin {

    public static final Logger LOGGER = LoggerFactory.getLogger(Lines2Bin.class);

    public static void main(String[] args) throws JSAPException, IOException {

        SimpleJSAP jsap = new SimpleJSAP(Lines2Bin.class.getName(), "",
                new Parameter[]{
                        new UnflaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "Input text file."),
                        new UnflaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "Output binary file.")
                });

        JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        try (DataOutputStream o = new DataOutputStream(new FileOutputStream(jsapResult.getString("output")))) {
            LineIterator iterator = FileUtils.lineIterator(new File(jsapResult.getString("input")));
            while (iterator.hasNext()) {
                String line = iterator.nextLine();
                long val = Long.valueOf(line);
                o.writeLong(val);
            }
        }

    }

}

package edu.nyu.tandon.experiments;

import com.martiansoftware.jsap.*;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.dsi.fastutil.floats.FloatBigArrayBigList;
import it.unimi.dsi.fastutil.floats.FloatBigList;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

/**
 * Created by juan on 11/15/16.
 */
public class DocHitsBySize {


	private static final Logger LOGGER = LoggerFactory.getLogger(edu.nyu.tandon.experiments.DocHitsBySize.class);

    private static final long serialVersionUID = 0L;

    public static void main(final String[] arg) throws JSAPException, IOException, ConfigurationException, SecurityException,
            URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException {

        final SimpleJSAP jsap = new SimpleJSAP(edu.nyu.tandon.tool.DocumentFeatures.class.getName(), "Extracts documental features.",
                new Parameter[]{
                        new FlaggedOption("docList", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'd', "docList", "A file of newline-separated, UTF-8 sorted document IDs"),
                        new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "The output filename ."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
                });

        JSAPResult jsapResult = jsap.parse(arg);
        if (jsap.messagePrinted()) return;

        final Index index = Index.getInstance(jsapResult.getString("basename"), false, true);

        // redirect output
        PrintStream output = (jsapResult.userSpecified("output")) ?
                new PrintStream(new FastBufferedOutputStream(new FileOutputStream(jsapResult.getString("output")))) :
                System.out;

        LOGGER.info("Reading document features for " + jsapResult.getString("basename"));

        BufferedReader doclist = new BufferedReader(new InputStreamReader(new FileInputStream(jsapResult.getString("docList")), Charset.forName("UTF-8")));
        String line;

        FloatBigList list = new FloatBigArrayBigList(index.numberOfDocuments);
        int n = 0;
        while ((line = doclist.readLine()) != null) {

            String[] tokens = line.split(",");
            if (tokens.length < 2) continue;

            long doc = Long.parseLong(tokens[0]);
            if (doc >= index.numberOfDocuments) continue;

            float hits = Float.parseFloat(tokens[1]);

            long size = index.sizes.get(doc);

            output.printf("%d,%f,\n", doc, hits / size);

            if ((n++ % 1000000) == 0)
                LOGGER.debug(Math.ceil(n / 1000000) + "M");
        }

    }
}


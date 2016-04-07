package edu.nyu.tandon.index.cluster;

/* MG4J - PruningLexicalStrategy
 *
 * Implement pruning by partitioning an index lexically into a two set partitioned index.
 * Assume set 0 is the pruned index, set 1 always returns null results (by implementing a Bloom filter that always returns false)
 *
 */

import com.martiansoftware.jsap.*;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.cluster.LexicalPartitioningStrategy;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.util.Properties;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Arrays;


/**
 * A lexical strategy that creates an index containing a subset of the terms.
 *
 * @author Juan Rodriguez
 */


public class PrunedLexicalStrategy implements LexicalPartitioningStrategy {

    static final long serialVersionUID = 0;
    private static final Logger LOGGER = LoggerFactory.getLogger(PrunedLexicalStrategy.class);
    public final Long2ObjectOpenHashMap<LongArrayList> localPostings;
    /**
     * The local number of each term.
     */
    private final Long2LongOpenHashMap localNumber;

    /**
     * Creates a new subset lexical strategy.
     *
     * @param postings subset of terms.
     */
    public PrunedLexicalStrategy(final Long2ObjectOpenHashMap<LongArrayList> postings) {

        final long[] t = postings.keySet().toLongArray();
        Arrays.sort(t);

        localNumber = new Long2LongOpenHashMap();
        localNumber.defaultReturnValue(-1);

        localPostings = new Long2ObjectOpenHashMap<LongArrayList>(postings.size());
        localPostings.defaultReturnValue(null);

        for (int i = 0; i < t.length; i++) {
            localNumber.put(t[i], i);
            localPostings.put(t[i], postings.get(t[i]));
        }
    }

    public static void main(final String[] arg) throws JSAPException, IOException, ConfigurationException, SecurityException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        final SimpleJSAP jsap = new SimpleJSAP(PrunedLexicalStrategy.class.getName(), "Builds a lexical partitioning strategy based on a prune list.",
                new Parameter[]{
                        new FlaggedOption("threshold", JSAP.DOUBLE_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 't', "threshold", "The prune threshold."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index."),
                        new UnflaggedOption("prunelist", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The ordered localPostings list"),
                        new UnflaggedOption("strategy", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename for the strategy.")
                });

        JSAPResult jsapResult = jsap.parse(arg);
        if (jsap.messagePrinted()) return;

        final Index index = Index.getInstance(jsapResult.getString("basename"));

        double threshold = Math.ceil(((double) index.numberOfPostings) * jsapResult.getDouble("threshold"));

        final Long2ObjectOpenHashMap<LongArrayList> postings = new Long2ObjectOpenHashMap<LongArrayList>();
        postings.defaultReturnValue(null);

        LOGGER.info("Generating posting prunning strategy for " + jsapResult.getString("basename"));

        // read the prune list up to 50%, we will be genrating prune levels at 1,2,3,4,5,10,15,20,25,30,35,40,50
        BufferedReader prunelist = new BufferedReader(new InputStreamReader(new FileInputStream(jsapResult.getString("prunelist")), Charset.forName("UTF-8")));
        double n = 0;
        double currSize;
        String line;
        while ((line = prunelist.readLine()) != null) {
            String[] tokens = line.split(",");
            if (tokens.length < 2) continue;
            long term = Long.parseLong(tokens[0]);
            long doc = Long.parseLong(tokens[1]);
            if (!postings.containsKey(term)) {
                postings.put(term, new LongArrayList());
            }

            LongArrayList V = postings.get(term);
            V.add(doc);
            if (n++ > threshold) break;
            if ((n % 10000000.0) == 0.0)
                LOGGER.info(jsapResult.getString("prunelist") + "... " + (int) Math.ceil(n / 1000000.0) + "M");
        }
        prunelist.close();

        BinIO.storeObject(new PrunedLexicalStrategy(postings), jsapResult.getString("strategy"));
        LOGGER.info("Strategy serialized: " + jsapResult.getString("strategy") + ": " + (int) n + " localPostings");

    }

    public int numberOfLocalIndices() {
        return 2;
    }

    public int localIndex(final long globalNumber) {
        return localNumber.get(globalNumber) == -1 ? 1 : 0;
    }

    public long localNumber(final long globalNumber) {
        long n = localNumber.get(globalNumber);
        return n == -1 ? 0 : n;
    }

    public LongArrayList localList(final long globalNumber) {
        return localPostings.get(globalNumber);
    }

    @Override
    /*
    TODO: fix this
	 */
    public Properties[] properties() {
        return null;
    }
}

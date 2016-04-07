package edu.nyu.tandon.index.cluster;

import com.martiansoftware.jsap.*;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.util.Properties;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Arrays;

public class PostingPruningStrategy implements DocumentalPartitioningStrategy, DocumentalClusteringStrategy, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostingPruningStrategy.class);

    private static final long serialVersionUID = 0L;
    /**
     * The (cached) number of segments.
     */
    private static final int k = 2;
    public final Long2ObjectOpenHashMap<LongOpenHashSet> localPostings;
    public final Long2LongOpenHashMap localDocuments;
    public final Long2LongOpenHashMap localTerms;

    /**
     * Creates a pruned strategy with the given localPostings
     */

    public PostingPruningStrategy(final Long2LongOpenHashMap terms, final Long2ObjectOpenHashMap<LongOpenHashSet> postings, Long2LongOpenHashMap docs) {

        if (terms.size() == 0 || docs.size() == 0) throw new IllegalArgumentException("Empty prune list");
        this.localTerms = terms.clone();
        this.localDocuments = docs.clone();
        this.localPostings = postings.clone();

    }

    public static void main(final String[] arg) throws JSAPException, IOException, ConfigurationException, SecurityException,
            URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException {

        final SimpleJSAP jsap = new SimpleJSAP(PrunedLexicalStrategy.class.getName(), "Builds a documental partitioning strategy based on a prune list.",
                new Parameter[]{
                        new FlaggedOption("threshold", JSAP.DOUBLE_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 't', "threshold", "Prune threshold for the index (may be specified several times).").setAllowMultipleDeclarations(true),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index."),
                        new UnflaggedOption("prunelist", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The ordered localPostings list"),
                        new UnflaggedOption("strategy", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename for the strategy.")
                });

        JSAPResult jsapResult = jsap.parse(arg);
        if (jsap.messagePrinted()) return;

        final Index index = Index.getInstance(jsapResult.getString("basename"));

        double[] t_list = jsapResult.getDoubleArray("threshold");
        if (t_list.length == 0) throw new IllegalArgumentException("You need to specify at least on ethreshold level.");
        Arrays.sort(t_list);
        double[] threshold = new double[t_list.length];
        boolean[] strategies = new boolean[t_list.length];
        for (int i = 0; i < t_list.length; i++) {
            threshold[i] = Math.ceil(((double) index.numberOfPostings) * t_list[i]);
            strategies[i] = true;
        }
        final Long2ObjectOpenHashMap<LongOpenHashSet> postings = new Long2ObjectOpenHashMap<LongOpenHashSet>();
        final Long2LongOpenHashMap terms = new Long2LongOpenHashMap();
        final Long2LongOpenHashMap documents = new Long2LongOpenHashMap();
        postings.defaultReturnValue(null);
        documents.defaultReturnValue(-1);
        terms.defaultReturnValue(-1);
        LOGGER.info("Generating posting prunning strategy for " + jsapResult.getString("basename"));

        // read the prune list up to 50%
        BufferedReader prunelist = new BufferedReader(new InputStreamReader(new FileInputStream(jsapResult.getString("prunelist")), Charset.forName("UTF-8")));
        double n = 0;
        String line;
        while ((line = prunelist.readLine()) != null) {

            String[] tokens = line.split(",");
            if (tokens.length < 2) continue;

            long doc = Long.parseLong(tokens[1]);
            long localDoc = documents.get(doc);
            if (localDoc == -1) {
                localDoc = documents.size();
                documents.put(doc, localDoc);
            }

            long term = Long.parseLong(tokens[0]);
            long localTerm = terms.get(term);
            if (localTerm == -1) {
                localTerm = terms.size();
                terms.put(term, localTerm);
            }

            // add to term list
            if (!postings.containsKey(term)) {
                // create a new term list
                postings.put(term, new LongOpenHashSet());
            }
            postings.get(term).add(doc);

            // dispatch intermediate strategies if we reached their thresholds
            for (int i = 0; i < t_list.length - 1; i++) {
                if (strategies[i] && n >= threshold[i]) {
                    strategies[i] = false;
                    BinIO.storeObject(new PostingPruningStrategy(terms, postings, documents), jsapResult.getString("strategy") + "." + String.valueOf(t_list[i]) + ".strategy");
                    LOGGER.info(String.valueOf(t_list[i]) + " strategy serialized : " + String.valueOf((int) Math.ceil(n / 1000000.0)) + "M localPostings");
                }
            }
            if (n++ >= threshold[threshold.length - 1]) break;

            if ((n % 10000000.0) == 0.0)
                LOGGER.info(jsapResult.getString("prunelist") + "... " + (int) Math.ceil(n / 1000000.0) + "M");
        }

        prunelist.close();

        // dump last one
        BinIO.storeObject(new PostingPruningStrategy(terms, postings, documents), jsapResult.getString("strategy") + "." + String.valueOf(t_list[t_list.length - 1]) + ".strategy");
        LOGGER.info(String.valueOf(t_list[t_list.length - 1]) + " strategy serialized : " + String.valueOf((int) Math.ceil(n / 1000000.0)) + "M localPostings");

    }

    /* pruned partitioning always creates 2 partitions: 0 and 1. 0 is the pruned one; all others are placed in partition 1 */
    public int numberOfLocalIndices() {
        return 2;
    }

    @Override
    /** return the index of the given document
     * @param globalPointer: global document ID
     */
    public int localIndex(final long globalPointer) {
        return (localDocuments.get(globalPointer) == -1) ? 1 : 0;
    }

    /**
     * return the index of the given posting
     *
     * @param term: global term id
     * @param doc:  global document ID
     */
    public int localIndex(final long term, final long doc) {
        if (localTerms.get(term) == -1) return 1;
        return (localPostings.get(term).contains(doc)) ? 0 : 1;
    }

    /**
     * return the local document ID
     *
     * @param globalPointer
     * @return
     */
    public long localPointer(final long globalPointer) {
        return localDocuments.get(globalPointer);
    }

    public long globalPointer(final int localIndex, final long localPointer) {
        if (localIndex == 1 || !localDocuments.containsValue(localPointer)) return -1;
        // find this value
        long d;
        for (long key : localDocuments.keySet()) {
            if ((d = localDocuments.get(key)) != -1)
                return d;
        }
        return -1;
    }

    public long numberOfDocuments(final int localIndex) {
        return (localIndex == 0) ? localDocuments.size() : 0;
    }

//	public String toString() {
//		return Arrays.toString( cutPoint );
//	}

    public Properties[] properties() {
//		Properties[] properties = new Properties[ k ];
//		for( int i = 0; i < k; i++ ) {
//			properties[ i ] = new Properties();
//			properties[ i ].addProperty( "pointerfrom", cutPoint[ i ] );
//			properties[ i ].addProperty( "pointerto", cutPoint[ i + 1 ] );
//		}
//		return properties;
        return (null);
    }
}

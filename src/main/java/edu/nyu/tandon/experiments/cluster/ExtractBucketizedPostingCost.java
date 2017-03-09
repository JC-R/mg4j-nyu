package edu.nyu.tandon.experiments.cluster;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.*;
import it.unimi.dsi.lang.MutableString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;
import java.util.stream.Collectors;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractBucketizedPostingCost {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractBucketizedPostingCost.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "The output files basename."),
                        new FlaggedOption("shardId", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "shard-id", "The shard ID."),
                        new FlaggedOption("buckets", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'b', "buckets", "The number of buckets."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");
        String[] basenameWeight = new String[]{basename};

        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<>();
        Query.loadIndicesFromSpec(basenameWeight, true, null, indexMap, index2Weight);

        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<>(indexMap.size());
        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);
        final SimpleParser simpleParser = new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors);
        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<>();

        Index index = indexMap.get(indexMap.firstKey());
        IndexReader indexReader = index.getReader();

        int bucketCount = jsapResult.getInt("buckets");
        long bucketSize = (long) Math.ceil(Long.valueOf(index.numberOfDocuments).doubleValue() / bucketCount);
        String outputBasename = jsapResult.userSpecified("shardId") ?
                String.format("%s#%d", jsapResult.getString("output"), jsapResult.getInt("shardId")) :
                jsapResult.getString("output");

        FileWriter[] writers = new FileWriter[bucketCount];
        for (int i = 0; i < bucketCount; i++) writers[i] = new FileWriter(String.format("%s#%d.postingcost", outputBasename, i));

        try(BufferedReader br = new BufferedReader(new FileReader(jsapResult.getString("input")))) {
            for (String query; (query = br.readLine()) != null; ) {

                TermProcessor termProcessor = termProcessors.get(indexMap.firstKey());
                List<String> processedTerms =
                        Lists.newArrayList(Splitter.on(' ').omitEmptyStrings().split(query))
                                .stream()
                                .map(t -> {
                                    MutableString m = new MutableString(t);
                                    termProcessor.processTerm(m);
                                    return m.toString();
                                })
                                .collect(Collectors.toList());

                long[] buckets = new long[bucketCount];
                for (String term : processedTerms) {
                    DocumentIterator it = indexReader.documents(term);
                    long docId;
                    while ((docId = it.nextDocument()) != END_OF_LIST) {
                        buckets[(int) (docId % bucketSize)]++;
                    }
                }
                for (int i = 0; i < bucketCount; i++) {
                    writers[i].append(String.format("%d\n", buckets[i]));
                }

            }
        }

        indexReader.close();
        for (FileWriter writer : writers) writer.close();

    }

}

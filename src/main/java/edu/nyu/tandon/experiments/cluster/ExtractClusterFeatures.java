package edu.nyu.tandon.experiments.cluster;

import com.github.elshize.bcsv.Header;
import com.github.elshize.bcsv.LineWriter;
import com.github.elshize.bcsv.column.ColumnType;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.query.QueryEngine;
import edu.nyu.tandon.shard.ranking.shrkc.node.Document;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.ClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.PartitioningStrategy;
import it.unimi.di.big.mg4j.index.cluster.SelectiveQueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.search.score.Scorer;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.*;
import it.unimi.dsi.lang.MutableString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.elshize.bcsv.column.ColumnType.*;
import static edu.nyu.tandon.query.Query.MAX_STEMMING;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractClusterFeatures {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractClusterFeatures.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new Switch("globalStatistics", 'g', "global-statistics", "Whether to use global statistics. Note that they need to be calculated: see ClusterGlobalStatistics."),
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "The output files basename."),
                        new FlaggedOption("topK", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'k', "top-k", "The engine will limit the result set to top k results. k=10 by default."),
                        new FlaggedOption("shardId", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "shard-id", "The shard ID."),
                        new FlaggedOption("scorer", JSAP.STRING_PARSER, "bm25", JSAP.NOT_REQUIRED, 'S', "scorer", "Scorer type (bm25 or ql)"),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        boolean shardDefined = jsapResult.userSpecified("shardId");

        String basename = jsapResult.getString("basename");
        String[] basenameWeight = new String[] { basename };

        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<>();
        Query.loadIndicesFromSpec(basenameWeight, true, null, indexMap, index2Weight);

        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<>(indexMap.size());
        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);
        final SimpleParser simpleParser = new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors);
        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<>();

        QueryEngine engine = new QueryEngine<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>>(
                simpleParser,
                new DocumentIteratorBuilderVisitor(indexMap, index2Parser, indexMap.get(indexMap.firstKey()), MAX_STEMMING),
                indexMap);
        engine.setWeights(index2Weight);
        Scorer scorer = ExtractShardScores.resolveScorer(jsapResult.getString("scorer"));
        if (jsapResult.userSpecified("globalStatistics")) {
            LOGGER.info("Running queries with global statistics.");
            SelectiveQueryEngine.setGlobalStatistics(scorer, basename);
        }
        engine.score(scorer);

        int k = jsapResult.userSpecified("topK") ? jsapResult.getInt("topK") : 10;
        String outputBasename = shardDefined ?
                String.format("%s#%d", jsapResult.getString("output"), jsapResult.getInt("shardId")) :
                jsapResult.getString("output");
        String type = shardDefined ? ".local" : ".global";

        DocumentalClusteringStrategy strategy = null;
        if (shardDefined) {
            strategy = (DocumentalClusteringStrategy) BinIO.loadObject(basename + ".strategy");
        }

        final int DOCUMENTS = 0;
        final int SCORES = 1;
        final int TIME = 2;
        final int MAX_LIST_1 = 3;
        final int MAX_LIST_2 = 4;
        final int MIN_LIST_1 = 5;
        final int MIN_LIST_2 = 6;
        final int SUM_LIST = 7;
        final int GLOBAL_IDS = 8;
        List<String> columnNames = new LinkedList<>(Arrays.asList(
                "documents",
                "scores",
                "time",
                "maxlist1",
                "maxlist2",
                "minlist1",
                "minlist2",
                "sumlist"));
        List<ColumnType> columnTypes = new LinkedList<>(Arrays.asList(
                listColumn(longColumn()),
                listColumn(doubleColumn()),
                longColumn(),
                longColumn(),
                longColumn(),
                longColumn(),
                longColumn(),
                longColumn()));
        if (shardDefined) {
            columnNames.add("globalids");
            columnTypes.add(listColumn(longColumn()));
        }
        Header header = new Header(columnNames.toArray(new String[]{}),
                columnTypes.toArray(new ColumnType[]{}));
        FileOutputStream out = new FileOutputStream(jsapResult.getString("output") + ".basefeatures");
        header.write(out);
        LineWriter writer = header.getLineWriter(out);

        long totalTime = 0;
        long queryCount = 0;
        try(BufferedReader br = new BufferedReader(new FileReader(jsapResult.getString("input")))) {
            for (String query; (query = br.readLine()) != null; ) {

                Object[] row = header.newRow();

                ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> r =
                        new ObjectArrayList<>();
                query = CharMatcher.is(',').replaceFrom(query, "");

                Index index = indexMap.get(indexMap.firstKey());
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
                List<Long> listLengths = processedTerms.stream().map(term -> {
                    try {
                        return index.documents(term).frequency();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());
                if (listLengths.isEmpty()) {
                    row[MAX_LIST_1] = 0;
                    row[MAX_LIST_2] = 0;
                    row[MIN_LIST_1] = 0;
                    row[MIN_LIST_2] = 0;
                }
                else {
                    row[MAX_LIST_1] = listLengths.get(0);
                    row[MIN_LIST_1] = listLengths.get(listLengths.size() - 1);
                    if (listLengths.size() > 1) {
                        row[MAX_LIST_2] = listLengths.get(1);
                        row[MIN_LIST_2] = listLengths.get(listLengths.size() - 2);
                    }
                    else {
                        row[MAX_LIST_2] = 0;
                        row[MIN_LIST_2] = 0;
                    }
                }
                row[SUM_LIST] = listLengths.stream().mapToLong(Long::longValue).sum();

                try {
                    long start = System.currentTimeMillis();
                    engine.process(query, 0, k, r);
                    long elapsed = System.currentTimeMillis() - start;
                    totalTime += elapsed;
                    queryCount++;
                    row[TIME] = elapsed;
                } catch (Exception e) {
                    LOGGER.error(String.format("There was an error while processing query: %s", query), e);
                    throw e;
                }

                Iterator<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> it = r.iterator();
                Object[] documents = new Object[r.size()];
                Object[] scores = new Object[r.size()];
                for (int i = 0; i < r.size(); i++) {
                    DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi = r.get(i);
                    documents[i] = dsi.document;
                    scores[i] = dsi.score;
                }
                row[DOCUMENTS] = documents;
                row[SCORES] = scores;
                if (shardDefined) {
                    Object[] globalIds = new Object[documents.length];
                    int shardId = jsapResult.getInt("shardId");
                    for (int i = 0; i < documents.length; i++) {
                        globalIds[i] = strategy.globalPointer(shardId, r.get(i).document);
                    }
                    row[GLOBAL_IDS] = globalIds;
                }

                writer.writeLine(row);
            }
        }

    }

}

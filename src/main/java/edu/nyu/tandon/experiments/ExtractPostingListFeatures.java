package edu.nyu.tandon.experiments;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.martiansoftware.jsap.*;
import edu.nyu.tandon.experiments.cluster.ExtractShardScores;
import edu.nyu.tandon.experiments.thrift.QueryFeatures;
import edu.nyu.tandon.experiments.thrift.Result;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.query.TerminatingQueryEngine;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.SelectiveQueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.search.score.Scorer;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.*;
import it.unimi.dsi.lang.MutableString;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.thrift.ThriftParquetWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.codehaus.plexus.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static edu.nyu.tandon.query.Query.MAX_STEMMING;
import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractPostingListFeatures {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractPostingListFeatures.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");
        String termFilePath = String.format("%s.terms", basename);

        Index index = Index.getInstance(basename, true, true, true);
        LineIterator terms = null;
        try (FileInputStream inputStream = new FileInputStream(termFilePath)) {
            terms = IOUtils.lineIterator(inputStream, StandardCharsets.UTF_8);
        }

        System.out.println("termid,length,maxscore");

        try (IndexReader indexReader = index.getReader()) {
            IndexIterator indexIterator;
            while ((indexIterator = indexReader.nextIterator()) != null) {
                indexIterator.term(terms.nextLine());
                double maxScore = 0.0;
                BM25Scorer scorer = new BM25Scorer();
                scorer.wrap(indexIterator);
                while (indexIterator.nextDocument() != END_OF_LIST) {
                    double score = scorer.score();
                    maxScore = Math.max(score, maxScore);
                }
                System.out.println(String.format("%d,%d,%f",
                        indexIterator.termNumber(),
                        indexIterator.frequency(),
                        maxScore));
            }
        }
    }

}

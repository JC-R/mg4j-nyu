package edu.nyu.tandon.experiments.cluster;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.search.score.BM25PrunedScorer;
import edu.nyu.tandon.search.score.QueryLikelihoodScorer;
import edu.nyu.tandon.shard.csi.CentralSampleIndex;
import edu.nyu.tandon.shard.ranking.ShardSelector;
import edu.nyu.tandon.shard.ranking.redde.ReDDEShardSelector;
import edu.nyu.tandon.shard.ranking.shrkc.RankS;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.search.score.Scorer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.codehaus.plexus.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;


/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractShardScores {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractShardScores.class);

    private static ShardSelector resolveShardSelector(String name, CentralSampleIndex csi, int base) {
        if ("redde".equals(name)) return new ReDDEShardSelector(csi);
        else if ("ranks".equals(name)) return new RankS(csi, base);
        else throw new IllegalArgumentException("You need to define a proper selector: redde, ranks");
    }

    public static Scorer resolveScorer(String name) {
        if ("bm25".equals(name)) return new BM25PrunedScorer();
        else if ("ql".equals(name)) return new QueryLikelihoodScorer();
        else throw new IllegalArgumentException("You need to define a proper scorer: bm25, ql");
    }

    public static List<Row> run(File input, String name, int clusters, ShardSelector shardSelector) throws IOException {

        StructType schema = new StructType()
                .add("query", IntegerType)
                .add("shard", IntegerType)
                .add(name, DoubleType);

        List<Row> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(input))) {
            int queryId = 0;
            for (String query; (query = br.readLine()) != null; ) {
                try {
                    Map<Integer, Double> shardScores = shardSelector.shardScores(query);
                    for (int shardId = 0; shardId < clusters; shardId++) {
                        rows.add(new GenericRowWithSchema(new Object[] {
                                queryId,
                                shardId,
                                shardScores.getOrDefault(shardId, 0.0)
                        }, schema));
                    }
                    queryId++;
                } catch (QueryParserException | QueryBuilderVisitorException | IOException e) {
                    throw new RuntimeException(String.format("There was an error while processing query: %s", query), e);
                }
            }
        }

        return rows;
        //return SparkSession.builder().master("local").getOrCreate().createDataFrame(rows, schema);
    }

    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), "Loads indices relative to a collection, possibly loads the collection, and answers to queries.",
                new Parameter[]{
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "The output files basename."),
                        new FlaggedOption("clusters", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'c', "clusters", "The number of clusters."),
                        new FlaggedOption("selector", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 's', "selector", "Selector type (redde or ranks)"),
                        new FlaggedOption("base", JSAP.INTEGER_PARSER, "2", JSAP.REQUIRED, 'b', "base", "The base for Rank-S."),
                        new FlaggedOption("scorer", JSAP.STRING_PARSER, "bm25", JSAP.NOT_REQUIRED, 'S', "scorer", "Scorer type (bm25 or ql)"),
                        new FlaggedOption("csiMaxOutput", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'L', "csi-max-output", "CSI maximal number of results")
                                .setAllowMultipleDeclarations(true),
                        new Switch("noIDConversion", 'n', "noconversion", "Do not use ID conversion."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the cluster indices (not including number suffixes). In other words, the basename of the partitioned index as if loaded as a DocumentalMergedCluster."),
                        new UnflaggedOption("csi", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the central sample index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        int clusters = jsapResult.getInt("clusters");

        LOGGER.info("Loading CSI...");
        CentralSampleIndex csi = CentralSampleIndex.loadCSI(jsapResult.getString("csi"),
                jsapResult.getString("basename"), resolveScorer(jsapResult.getString("scorer")),
                !jsapResult.userSpecified("noIDConversion"));

        int[] csiMaxOutputs;
        if (jsapResult.userSpecified("csiMaxOutput")) csiMaxOutputs = jsapResult.getIntArray("csiMaxOutput");
        else csiMaxOutputs = new int[] { 100 };
        String selector = jsapResult.getString("selector");

        SchemaBuilder.FieldAssembler fields = SchemaBuilder
                .record(selector)
                .namespace("edu.nyu.tandon.experiments.avro")
                .fields()
                .name("query").type().intType().noDefault()
                .name("shard").type().intType().noDefault();

        for (int L : csiMaxOutputs) {
            fields = (SchemaBuilder.FieldAssembler)
                    fields.name(selector + "_" + L).type().optional().doubleType();
        }

        Schema schema = (Schema) fields.endRecord();

        List<List<Row>> datasets = new ArrayList<>();

        for (int L : csiMaxOutputs) {

            String name = jsapResult.getString("selector") + "_" + L;


            LOGGER.info(String.format("Extracting shard scores for L = %d", L));
            csi.setMaxOutput(L);
            ShardSelector shardSelector = resolveShardSelector(selector,
                    csi, jsapResult.getInt("base"));

            List<Row> df = run(new File(jsapResult.getString("input")), name, clusters, shardSelector);
            datasets.add(df);

        }

        String outputFile = jsapResult.getString("output") + "." + selector;
        if (FileUtils.fileExists(outputFile)) {
            org.apache.commons.io.FileUtils.forceDelete(new File(outputFile));
        }
        int rowCount = datasets.get(0).size();
        AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<>(
                new org.apache.hadoop.fs.Path(outputFile), schema);


        for (int row = 0; row < rowCount; row++) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            builder.set("query", datasets.get(0).get(row).getInt(0));
            builder.set("shard", datasets.get(0).get(row).getInt(1));
            for (int i = 0; i < csiMaxOutputs.length; i++) {
                builder = builder.set(selector + "_" + csiMaxOutputs[i], datasets.get(i).get(row).getDouble(2));
            }
            writer.write(builder.build());
        }

        writer.close();
    }

}

package edu.nyu.tandon.experiments.cluster;

import edu.nyu.tandon.test.BaseTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractClusterFeaturesTest extends BaseTest {

    @Test
    public void runQueriesAndCheckFileFormat() throws Exception {

        // Given
        String outputBasename = temporaryFolder.getRoot().getAbsolutePath() + "/test";
        String[] args = String.format("-i %s -o %s -k 15 %s",
                getFileFromResourcePath("queries/gov2-trec_eval-queries.txt").getAbsoluteFile(),
                outputBasename,
                getFileFromResourcePath("index").getAbsoluteFile() + "/gov2-text")
                .split(" ");

        // When
        ExtractClusterFeatures.main(args);

        // Then
        assertTrue(Files.exists(Paths.get(outputBasename + ".results")));
        assertTrue(Files.exists(Paths.get(outputBasename + ".queryfeatures")));
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        Dataset results = spark.read().parquet(outputBasename + ".results");
        assertThat(Arrays.asList(results.columns()),
                containsInAnyOrder("query", "docid-local", "docid-global", "score", "ridx"));
        Dataset queryFeatures = spark.read().parquet(outputBasename + ".queryfeatures");
        assertThat(Arrays.asList(queryFeatures.columns()),
                containsInAnyOrder("query", "time", "maxlist1", "maxlist2", "minlist1", "minlist2", "sumlist"));
        assertThat(queryFeatures.count(), equalTo(150L));
    }

    @Test
    public void globalStatistics() throws Exception {

        // Given
        String outputBasename = temporaryFolder.getRoot().getAbsolutePath() + "/test";
        String[] args = String.format("-g -i %s -o %s -k 15 -s %d %s",
                getFileFromResourcePath("queries/gov2-trec_eval-queries.txt").getAbsoluteFile(),
                outputBasename,
                0,
                getFileFromResourcePath("clusters").getAbsoluteFile() + "/gov2C-0")
                .split(" ");

        // When
        ExtractClusterFeatures.main(args);

        // Then
        assertTrue(Files.exists(Paths.get(outputBasename + "#0.results")));
        assertTrue(Files.exists(Paths.get(outputBasename + "#0.queryfeatures")));
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        Dataset results = spark.read().parquet(outputBasename + "#0.results");
        assertThat(Arrays.asList(results.columns()),
                containsInAnyOrder("query", "shard", "docid-local", "docid-global", "score", "ridx"));
        Dataset queryFeatures = spark.read().parquet(outputBasename + "#0.queryfeatures");
        assertThat(Arrays.asList(queryFeatures.columns()),
                containsInAnyOrder("query", "shard", "time", "maxlist1", "maxlist2", "minlist1", "minlist2", "sumlist"));
        assertThat(queryFeatures.count(), equalTo(150L));
    }

    @Test
    public void runWithBuckets() throws Exception {

        // Given
        String outputBasename = temporaryFolder.getRoot().getAbsolutePath() + "/test";
        String[] args = String.format("-g -i %s -o %s -k 15 -s %d -b 2 %s",
                getFileFromResourcePath("queries/gov2-trec_eval-queries.txt").getAbsoluteFile(),
                outputBasename,
                0,
                getFileFromResourcePath("clusters").getAbsoluteFile() + "/gov2C-0")
                .split(" ");

        // When
        ExtractClusterFeatures.main(args);

        // Then
        assertTrue(Files.exists(Paths.get(outputBasename + "#0.results-2")));
        assertTrue(Files.exists(Paths.get(outputBasename + "#0.queryfeatures")));
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        Dataset results = spark.read().parquet(outputBasename + "#0.results-2");
        assertThat(Arrays.asList(results.columns()),
                containsInAnyOrder("query", "shard", "docid-local", "docid-global", "score", "bucket", "ridx"));
        Dataset queryFeatures = spark.read().parquet(outputBasename + "#0.queryfeatures");
        assertThat(Arrays.asList(queryFeatures.columns()),
                containsInAnyOrder("query", "shard", "time", "maxlist1", "maxlist2", "minlist1", "minlist2", "sumlist"));
        assertThat(queryFeatures.count(), equalTo(150L));
    }

}

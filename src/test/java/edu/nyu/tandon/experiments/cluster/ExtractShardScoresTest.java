package edu.nyu.tandon.experiments.cluster;

import com.google.common.primitives.Doubles;
import edu.nyu.tandon.test.BaseTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractShardScoresTest extends BaseTest {

    @Test
    public void redde() throws Exception {

        // Given
        File input = newTemporaryFileWithContent("the\na\n");
        String outputBasename = temporaryFolder.getRoot().getAbsolutePath() + "/test";
        String[] args = String.format("-i %s -o %s -c 11 -s redde %s %s",
                input.getAbsoluteFile(),
                outputBasename,
                getFileFromResourcePath("clusters").getAbsoluteFile() + "/gov2C",
                getFileFromResourcePath("csi").getAbsoluteFile() + "/csi")
                .split(" ");

        // When
        ExtractShardScores.main(args);

        // Then
        assertTrue(Files.exists(Paths.get(outputBasename + ".redde")));
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        Dataset redde = spark.read().parquet(outputBasename + ".redde");
        assertThat(redde.columns(), equalTo(new String[] {"query", "shard", "redde-100"}));
        assertThat(redde.count(), equalTo(22L));
    }

    @Test
    public void rankS() throws Exception {

        // Given
        File input = newTemporaryFileWithContent("the\na\n");
        String outputBasename = temporaryFolder.getRoot().getAbsolutePath() + "/test";
        String[] args = String.format("-i %s -o %s -c 11 -s ranks %s %s",
                input.getAbsoluteFile(),
                outputBasename,
                getFileFromResourcePath("clusters").getAbsoluteFile() + "/gov2C",
                getFileFromResourcePath("csi").getAbsoluteFile() + "/csi")
                .split(" ");

        // When
        ExtractShardScores.main(args);

        // Then
        assertTrue(Files.exists(Paths.get(outputBasename + ".ranks")));
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        Dataset ranks = spark.read().parquet(outputBasename + ".ranks");
        assertThat(ranks.columns(), equalTo(new String[] {"query", "shard", "ranks-100"}));
        assertThat(ranks.count(), equalTo(22L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void unknownSelector() throws Exception {

        String[] args = String.format("-i %s -o x -c 11 -s unknown %s %s",
                "x",
                getFileFromResourcePath("clusters").getAbsoluteFile() + "/gov2C",
                getFileFromResourcePath("csi").getAbsoluteFile() + "/csi")
                .split(" ");
        ExtractShardScores.main(args);

    }

}

package edu.nyu.tandon.experiments.cluster;

import com.google.common.primitives.Doubles;
import edu.nyu.tandon.test.BaseTest;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

import static com.google.common.primitives.Ints.tryParse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractClusterFeaturesTest extends BaseTest {

    //@Test
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
        int count = 0;
        for (String t : Files.readAllLines(Paths.get(outputBasename + ".results.global"))) {
            if (!"".equals(t)) {
                String[] l = t.split(" ");
                for (String r : l) assertThat(tryParse(r), notNullValue(Integer.class));
            }
            count++;
        }
        assertThat(count, equalTo(150));
        count = 0;
        for (String t : Files.readAllLines(Paths.get(outputBasename + ".results.scores"))) {
            if (!"".equals(t)) {
                String[] l = t.split(" ");
                for (String r : l) assertThat(Doubles.tryParse(r), notNullValue(Double.class));
            }
            count++;
        }
        assertThat(count, equalTo(150));
    }

    //@Test
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
        int count = 0;
        for (String t : Files.readAllLines(Paths.get(outputBasename + "#0.results.local"))) {
            if (!"".equals(t)) {
                String[] l = t.split(" ");
                for (String r : l) assertThat(tryParse(r), notNullValue(Integer.class));
            }
            count++;
        }
        assertThat(count, equalTo(150));
        count = 0;
        for (String t : Files.readAllLines(Paths.get(outputBasename + "#0.results.scores"))) {
            if (!"".equals(t)) {
                String[] l = t.split(" ");
                for (String r : l) assertThat(Doubles.tryParse(r), notNullValue(Double.class));
            }
            count++;
        }
        assertThat(count, equalTo(150));
    }

}

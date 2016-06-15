package edu.nyu.tandon.experiments;

import edu.nyu.tandon.test.BaseTest;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static com.google.common.primitives.Ints.tryParse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class RunQueriesTest extends BaseTest {

    @Test
    public void runQueriesAndCheckFileFormat() throws Exception {

        // Given
        File outputTime = newTemporaryFile();
        File outputResult = newTemporaryFile();
        String[] args = String.format("-i %s -t %s -r %s %s",
                getFileFromResourcePath("queries/gov2-trec_eval-queries.txt").getAbsoluteFile(),
                outputTime.getAbsoluteFile(),
                outputResult.getAbsoluteFile(),
                getFileFromResourcePath("clusters").getAbsoluteFile() + "/gov2C-5")
                .split(" ");

        // When
        RunQueries.main(args);

        // Then
        int count = 0;
        for (String t : Files.readAllLines(outputTime.toPath())) {
            assertThat(tryParse(t), notNullValue(Integer.class));
            count++;
        }
        assertThat(count, equalTo(150));
    }

    @Test
    public void globalStatistics() throws Exception {

        // Given
        File outputTime = newTemporaryFile();
        File outputResult = newTemporaryFile();
        String[] args = String.format("-g -i %s -t %s -r %s %s",
                getFileFromResourcePath("queries/gov2-trec_eval-queries.txt").getAbsoluteFile(),
                outputTime.getAbsoluteFile(),
                outputResult.getAbsoluteFile(),
                getFileFromResourcePath("clusters").getAbsoluteFile() + "/gov2C-5")
                .split(" ");

        // When
        RunQueries.main(args);

        // Then
        int count = 0;
        for (String t : Files.readAllLines(outputTime.toPath())) {
            assertThat(tryParse(t), notNullValue(Integer.class));
            count++;
        }
        assertThat(count, equalTo(150));
    }

}

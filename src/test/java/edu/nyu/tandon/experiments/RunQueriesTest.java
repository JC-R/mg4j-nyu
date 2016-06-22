package edu.nyu.tandon.experiments;

import com.google.common.base.Splitter;
import edu.nyu.tandon.test.BaseTest;
import org.hamcrest.CoreMatchers;
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
        File outputListLengths = newTemporaryFile();
        String[] args = String.format("-i %s -t %s -r %s -l %s -k 15 %s",
                getFileFromResourcePath("queries/gov2-trec_eval-queries.txt").getAbsoluteFile(),
                outputTime.getAbsoluteFile(),
                outputResult.getAbsoluteFile(),
                outputListLengths.getAbsoluteFile(),
                getFileFromResourcePath("index").getAbsoluteFile() + "/gov2-text")
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
        count = 0;
        for (String l : Files.readAllLines(outputResult.toPath())) {
            for (String doc : Splitter.on(" ").omitEmptyStrings().split(" ")) {
                assertThat(String.format("Problem parsing list of integers: %s", l),
                        tryParse(doc), notNullValue(Integer.class));
            }
            count++;
        }
        assertThat(count, equalTo(150));
        count = 0;
        for (String l : Files.readAllLines(outputListLengths.toPath())) {
            for (String length : Splitter.on(" ").omitEmptyStrings().split(" ")) {
                Integer len = tryParse(length);
                assertThat(String.format("Problem parsing list of integers: %s", l),
                        tryParse(length), notNullValue(Integer.class));
                assertThat(String.format("There is a -1 value (exception during running queries) at line %d", count),
                        len, CoreMatchers.not(equalTo(-1l)));
            }
            count++;
        }
        assertThat(count, equalTo(150));
    }

    @Test
    public void globalStatistics() throws Exception {

        // Given
        File outputTime = newTemporaryFile();
        File outputResult = newTemporaryFile();
        File outputListLengths = newTemporaryFile();
        String[] args = String.format("-g -i %s -t %s -r %s -l %s %s",
                getFileFromResourcePath("queries/gov2-trec_eval-queries.txt").getAbsoluteFile(),
                outputTime.getAbsoluteFile(),
                outputResult.getAbsoluteFile(),
                outputListLengths.getAbsoluteFile(),
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

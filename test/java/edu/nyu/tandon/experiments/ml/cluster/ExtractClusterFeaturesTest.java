package edu.nyu.tandon.experiments.ml.cluster;

import com.google.common.base.Splitter;
import edu.nyu.tandon.test.BaseTest;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Iterator;

import static com.google.common.primitives.Ints.tryParse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractClusterFeaturesTest extends BaseTest {

    @Test
    public void runQueriesAndCheckFileFormat() throws Exception {

        // Given
        File outputTime = newTemporaryFile();
        File outputResult = newTemporaryFile();
        File outputListLengths = newTemporaryFile();
        String[] args = String.format("-i %s -t %s -r %s -l %s -k 15 -c 0 %s",
                getFileFromResourcePath("queries/gov2-trec_eval-queries.txt").getAbsoluteFile(),
                outputTime.getAbsoluteFile(),
                outputResult.getAbsoluteFile(),
                outputListLengths.getAbsoluteFile(),
                getFileFromResourcePath("index").getAbsoluteFile() + "/gov2-text")
                .split(" ");

        // When
        ExtractClusterFeatures.main(args);

        // Then
        int count = 0;
        for (String t : Files.readAllLines(outputTime.toPath())) {
            if (count == 0) assertThat(t, equalTo("id,cluster,time"));
            else {
                String[] l = t.split(",");
                assertThat(tryParse(l[0]), notNullValue(Integer.class));
                assertThat(tryParse(l[1]), notNullValue(Integer.class));
                assertThat(tryParse(l[2]), notNullValue(Integer.class));
            }
            count++;
        }
        assertThat(count, equalTo(151));
        count = 0;
        for (String line : Files.readAllLines(outputResult.toPath())) {
            if (count == 0) assertThat(line, equalTo("id,cluster,results"));
            else {
                Iterator<String> l = Splitter.on(",").split(line).iterator();
                assertThat(tryParse(l.next()), notNullValue(Integer.class));
                assertThat(tryParse(l.next()), notNullValue(Integer.class));
                String s = l.next();
                for (String doc : Splitter.on(" ").omitEmptyStrings().split(s)) {
                    assertThat(String.format("Problem parsing list of integers: %s", s),
                            tryParse(doc), notNullValue(Integer.class));
                }
            }
            count++;
        }
        assertThat(count, equalTo(151));
        count = 0;
        for (String line : Files.readAllLines(outputListLengths.toPath())) {
            if (count == 0) assertThat(line, equalTo("id,cluster,list-lengths"));
            else {
                String[] l = line.split(",");
                assertThat(tryParse(l[0]), notNullValue(Integer.class));
                assertThat(tryParse(l[1]), notNullValue(Integer.class));
                for (String length : Splitter.on(" ").omitEmptyStrings().split(l[2])) {
                    Integer len = tryParse(length);
                    assertThat(String.format("Problem parsing list of integers: %s", l[2]),
                            tryParse(length), notNullValue(Integer.class));
                    assertThat(String.format("There is a -1 value (exception during running queries) at line %d", count),
                            len, CoreMatchers.not(equalTo(-1l)));
                }
            }
            count++;
        }
        assertThat(count, equalTo(151));
    }

    @Test
    public void globalStatistics() throws Exception {

        // Given
        File outputTime = newTemporaryFile();
        File outputResult = newTemporaryFile();
        File outputListLengths = newTemporaryFile();
        String[] args = String.format("-g -i %s -t %s -r %s -l %s -c 0 %s",
                getFileFromResourcePath("queries/gov2-trec_eval-queries.txt").getAbsoluteFile(),
                outputTime.getAbsoluteFile(),
                outputResult.getAbsoluteFile(),
                outputListLengths.getAbsoluteFile(),
                getFileFromResourcePath("clusters").getAbsoluteFile() + "/gov2C-5")
                .split(" ");

        // When
        ExtractClusterFeatures.main(args);

        // Then
        int count = 0;
        for (String t : Files.readAllLines(outputTime.toPath())) {
            count++;
        }
        assertThat(count, equalTo(151));
    }

}

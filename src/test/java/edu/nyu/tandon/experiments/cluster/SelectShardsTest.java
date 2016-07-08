package edu.nyu.tandon.experiments.cluster;

import edu.nyu.tandon.experiments.cluster.SelectShards;
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
public class SelectShardsTest extends BaseTest {

    @Test
    public void runQueriesAndCheckFileFormat() throws Exception {

        // Given
        File outputTime = newTemporaryFile();
        File outputResult = newTemporaryFile();
        String[] args = String.format("-i %s -t %s -r %s -c 0 %s %s",
                    getFileFromResourcePath("queries/gov2-trec_eval-queries.txt").getAbsoluteFile(),
                    outputTime.getAbsoluteFile(),
                    outputResult.getAbsoluteFile(),
                    getFileFromResourcePath("clusters").getAbsoluteFile() + "/gov2C",
                    getFileFromResourcePath("csi").getAbsoluteFile() + "/csi")
                .split(" ");

        // When
        SelectShards.main(args);

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
    }

}

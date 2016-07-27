package edu.nyu.tandon.experiments.cluster;

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
public class ExtractShardScoresTest extends BaseTest {

    @Test
    public void redde() throws Exception {

        // Given
        File input = newTemporaryFileWithContent("the\n");
        File outputTime = newTemporaryFile();
        File outputScores = newTemporaryFile();
        String[] args = String.format("-i %s -t %s -r %s -c 11 -s redde %s %s",
                input.getAbsoluteFile(),
                outputTime.getAbsoluteFile(),
                outputScores.getAbsoluteFile(),
                getFileFromResourcePath("clusters").getAbsoluteFile() + "/gov2C",
                getFileFromResourcePath("csi").getAbsoluteFile() + "/csi")
                .split(" ");

        // When
        ExtractShardScores.main(args);

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
        assertThat(count, equalTo(12));
    }

    @Test
    public void shrkc() throws Exception {

        // Given
        File input = newTemporaryFileWithContent("the\n");
        File outputTime = newTemporaryFile();
        File outputScores = newTemporaryFile();
        String[] args = String.format("-i %s -t %s -r %s -c 11 -s shrkc %s %s",
                input.getAbsoluteFile(),
                outputTime.getAbsoluteFile(),
                outputScores.getAbsoluteFile(),
                getFileFromResourcePath("clusters").getAbsoluteFile() + "/gov2C",
                getFileFromResourcePath("csi").getAbsoluteFile() + "/csi")
                .split(" ");

        // When
        ExtractShardScores.main(args);

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
        assertThat(count, equalTo(12));
    }

    @Test(expected = IllegalArgumentException.class)
    public void unknownSelector() throws Exception {

        String[] args = String.format("-i %s -c 11 -s unknown %s %s",
                "",
                getFileFromResourcePath("clusters").getAbsoluteFile() + "/gov2C",
                getFileFromResourcePath("csi").getAbsoluteFile() + "/csi")
                .split(" ");
        ExtractShardScores.main(args);
        
    }

}

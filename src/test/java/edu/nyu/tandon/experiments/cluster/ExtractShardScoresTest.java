package edu.nyu.tandon.experiments.cluster;

import com.google.common.primitives.Doubles;
import edu.nyu.tandon.test.BaseTest;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractShardScoresTest extends BaseTest {

    //@Test
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
        for (int i = 0; i < 11; i++) {
            for (String l : Files.readAllLines(Paths.get(String.format("%s#%d.redde-100", outputBasename, i)))) {
                Double score = Doubles.tryParse(l);
                assertThat(score, notNullValue());
                assertThat(score >= 0, equalTo(Boolean.TRUE));
            }
        }
    }

    //@Test
    public void shrkc() throws Exception {

        // Given
        File input = newTemporaryFileWithContent("the\na\n");
        String outputBasename = temporaryFolder.getRoot().getAbsolutePath() + "/test";
        String[] args = String.format("-i %s -o %s -c 11 -s shrkc %s %s",
                input.getAbsoluteFile(),
                outputBasename,
                getFileFromResourcePath("clusters").getAbsoluteFile() + "/gov2C",
                getFileFromResourcePath("csi").getAbsoluteFile() + "/csi")
                .split(" ");

        // When
        ExtractShardScores.main(args);

        // Then
        for (int i = 0; i < 11; i++) {
            for (String l : Files.readAllLines(Paths.get(String.format("%s#%d.shrkc-100", outputBasename, i)))) {
                assertThat(Doubles.tryParse(l), notNullValue());
            }
        }
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

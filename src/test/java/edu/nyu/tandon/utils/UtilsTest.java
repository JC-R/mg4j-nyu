package edu.nyu.tandon.utils;

import it.unimi.di.big.mg4j.index.snowball.EnglishStemmer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class UtilsTest {

    @Test
    public void extractTerms() {
        List<String> actual = Utils.extractTerms("a OR b OR something, else", new EnglishStemmer());
        assertThat(actual, equalTo(Arrays.asList("a", "b", "someth", "els")));
    }

}

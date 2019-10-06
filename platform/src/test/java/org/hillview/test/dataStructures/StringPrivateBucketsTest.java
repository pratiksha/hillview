package org.hillview.test.dataStructures;

import org.hillview.privacy.StringPrivacyMetadata;
import org.hillview.sketches.StringPrivateBuckets;
import org.hillview.sketches.TreeHistogramBuckets;
import org.hillview.test.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StringPrivateBucketsTest extends BaseTest {
    @Test
    public void testBasicStringBuckets() {
        final int numBuckets = 10;
        final double epsilon = 0.01;

        String[] leafLeftBoundaries = new String[10];
        leafLeftBoundaries[0] = "a";
        leafLeftBoundaries[1] = "b";
        leafLeftBoundaries[2] = "c";
        leafLeftBoundaries[3] = "d";
        leafLeftBoundaries[4] = "e";
        leafLeftBoundaries[5] = "f";
        leafLeftBoundaries[6] = "g";
        leafLeftBoundaries[7] = "h";
        leafLeftBoundaries[8] = "i";
        leafLeftBoundaries[9] = "j";
        String max = "k";

        TreeHistogramBuckets buckDes = new StringPrivateBuckets("a", "k",
                numBuckets, new StringPrivacyMetadata(epsilon, leafLeftBoundaries, max));

        assertEquals(buckDes.indexOf("defjh"), 3);
    }
}

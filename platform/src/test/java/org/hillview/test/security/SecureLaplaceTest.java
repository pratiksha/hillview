package org.hillview.test.security;

import org.hillview.dataset.api.Pair;
import org.hillview.security.SecureLaplace;
import org.hillview.test.BaseTest;
import org.junit.Test;

import java.nio.file.Paths;

public class SecureLaplaceTest extends BaseTest {
    @Test
    public void LaplaceTest() {
        SecureLaplace sl = new SecureLaplace(Paths.get("./hillview_test_key"));
        double scale = 10;
        Pair<Integer, Integer> idx = new Pair<>(10, 11);
        double noise = sl.sampleLaplace(idx, scale);
        System.out.println(noise);
    }
}

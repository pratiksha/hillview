package org.hillview.test.security;

import org.hillview.dataset.api.Pair;
import org.hillview.security.SecureLaplace;
import org.hillview.security.TestKeyLoader;
import org.hillview.test.BaseTest;
import org.junit.Test;

public class SecureLaplaceTest extends BaseTest {
    @Test
    public void LaplaceTest() {
        TestKeyLoader tkl = new TestKeyLoader();
        SecureLaplace sl = new SecureLaplace(tkl);
        double scale = 10;
        Pair<Integer, Integer> idx = new Pair<>(10, 11);
        double noise = sl.sampleLaplace(-1, scale, idx);
        System.out.println(noise);
    }

    @Test
    public void laplaceVarianceTest() {
        TestKeyLoader tkl = new TestKeyLoader();

        double scale = 10;
        double trueVar = 2*Math.pow(scale, 2);
        Pair<Integer, Integer> idx = new Pair<>(10, 11);
        int n = 1000000;
        double sqsum = 0.0;
        double sum = 0.0;
        for (int i = 0; i < n; i++) {
            tkl.setIndex(i);
            SecureLaplace sl = new SecureLaplace(tkl);

            double noise = sl.sampleLaplace(-1, scale, idx);
            System.out.println(noise);
            sum += noise;
            sqsum += Math.pow(noise, 2);
        }
        double sampleVar = sqsum - Math.pow(sum, 2) / n;
        sampleVar /= n-1;

        assert(Math.abs(sampleVar - trueVar) < 1.0);
    }
}

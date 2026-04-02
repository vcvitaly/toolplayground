package io.github.vcvitaly.isolationplayground;

import lombok.experimental.UtilityClass;

@UtilityClass
public class TestUtil {

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

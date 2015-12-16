package com.gpjpe;

import java.util.Arrays;
import java.util.List;

public enum TOPOLOGY_DATA_SOURCE {
    INTERNAL,
    TWITTER;

    public static List<String> modes() {
        return Arrays.asList(INTERNAL.name(), TWITTER.name());
    }
}

package com.gpjpe;

import java.util.Arrays;
import java.util.List;

public enum TopologyDataSource {
    INTERNAL,
    KAFKA;

    public static List<String> modes() {
        return Arrays.asList(INTERNAL.name(), KAFKA.name());
    }
}

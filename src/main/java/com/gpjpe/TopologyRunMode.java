package com.gpjpe;

import java.util.Arrays;
import java.util.List;

public enum TopologyRunMode {
    LOCAL,
    REMOTE;

    public static List<String> modes() {
        return Arrays.asList(LOCAL.name(), REMOTE.name());
    }
}

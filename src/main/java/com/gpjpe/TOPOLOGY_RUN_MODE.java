package com.gpjpe;

import java.util.Arrays;
import java.util.List;

public enum TOPOLOGY_RUN_MODE {
    LOCAL,
    REMOTE;

    public static List<String> modes() {
        return Arrays.asList(LOCAL.name(), REMOTE.name());
    }
}

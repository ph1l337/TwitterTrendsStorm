package com.gpjpe.domain;

import java.util.HashMap;
import java.util.Map;

public class WindowHashTagTally {

    private Map<Long, Map<String, Long>> tally;

    public WindowHashTagTally(){
        this.tally = new HashMap<>();
    }

    public void add(String hashTag, Long window){

        Map<String, Long> windowTally = this.tally.get(window);

        if (windowTally == null){
            windowTally = new HashMap<>();
            this.tally.put(window, windowTally);
        }

            Long count = windowTally.get(hashTag);

        if (count == null){
            windowTally.put(hashTag, 1L);
        }else{
            windowTally.put(hashTag, count + 1L);
        }
    }

    public Map<String, Long> getWindowTally(Long window){
        return this.tally.get(window);
    }

    public void removeWindow(Long window){
        this.tally.remove(window);
    }
}

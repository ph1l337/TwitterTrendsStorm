package com.gpjpe.domain;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;


public class HashtagCountComparatorTest extends TestCase {
    private final static Logger LOGGER = LoggerFactory.getLogger(HashtagCountComparatorTest.class.getName());
    Comparator comparator = new HashtagCountComparator();
    HashtagCount a = new HashtagCount("a",10L);
    HashtagCount aa = new HashtagCount("a",10L);
    HashtagCount b = new HashtagCount("b",10L);
    HashtagCount c = new HashtagCount("c",11L);



    public void testComparison(){
      assertEquals(1, comparator.compare(a,b));
      assertEquals(0, comparator.compare(a,aa));
      assertEquals(-1, comparator.compare(b,a));
      assertEquals(-1, comparator.compare(b,c));
    }

}

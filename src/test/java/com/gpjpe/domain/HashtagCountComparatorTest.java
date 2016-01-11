package com.gpjpe.domain;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;


public class HashtagCountComparatorTest extends TestCase {
    private final static Logger LOGGER = LoggerFactory.getLogger(HashtagCountComparatorTest.class.getName());
    Comparator comparator = new HashtagCountComparator();
    HashtagCount a = new HashtagCount("a", 10L);
    HashtagCount aa = new HashtagCount("a", 10L);
    HashtagCount b = new HashtagCount("b", 10L);
    HashtagCount c = new HashtagCount("c", 11L);

    public void testComparison() {
        assertEquals(1, comparator.compare(a, b));
        assertEquals(0, comparator.compare(a, aa));
        assertEquals(-1, comparator.compare(b, a));
        assertEquals(-1, comparator.compare(b, c));
        assertEquals(-1, comparator.compare(new HashtagCount("Abc", 10L), new HashtagCount("abc", 10L)));
        assertEquals(1, comparator.compare(new HashtagCount("abc", 10L), new HashtagCount("Abc", 10L)));
        assertEquals(1, comparator.compare(new HashtagCount("1abc", 10L), new HashtagCount("abc", 10L)));
        assertEquals(1, comparator.compare(new HashtagCount("a1bc", 10L), new HashtagCount("a2bc", 10L)));
        assertEquals(1, comparator.compare(new HashtagCount("1", 10L), new HashtagCount("2", 10L)));
        assertEquals(1, comparator.compare(new HashtagCount("1", 10L), new HashtagCount("2", 8L)));
        assertEquals(1, comparator.compare(new HashtagCount("2", 10L), new HashtagCount("1", 8L)));
        assertEquals(-1, comparator.compare(new HashtagCount("1", 8L), new HashtagCount("2", 10L)));
    }
}

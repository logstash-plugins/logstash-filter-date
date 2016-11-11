package org.logstash.filters;

import org.junit.Assert;
import org.junit.Test;
import org.logstash.Event;
import org.logstash.Timestamp;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DateFilterTest {
    private List<String> failtagList = Collections.singletonList("_date_parse_fail");
    private String tz = "UTC";
    private String loc = "en";

    @Test
    public void testIsoStrings() throws Exception {

        Map<String, String> testElements = new HashMap<String, String>() {{
            put("2001-01-01T00:00:00-0800", "2001-01-01T08:00:00.000Z");
            put("1974-03-02T04:09:09-0800", "1974-03-02T12:09:09.000Z");
            put("2010-05-03T08:18:18+00:00", "2010-05-03T08:18:18.000Z");
            put("2004-07-04T12:27:27-00:00", "2004-07-04T12:27:27.000Z");
            put("2001-09-05T16:36:36+0000", "2001-09-05T16:36:36.000Z");
            put("2001-11-06T20:45:45-0000", "2001-11-06T20:45:45.000Z");
            put("2001-12-07T23:54:54Z", "2001-12-07T23:54:54.000Z");
        }};
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]", failtagList);
        subject.acceptFilterConfig("ISO8601", loc, tz);
        for (Map.Entry<String, String> entry : testElements.entrySet()) {
            applyString(subject, entry.getKey(), entry.getValue());
        }
    }

    @Test
    public void testTai64Strings() throws Exception {
        Map<String, String> testElements = new HashMap<String, String>() {{
            put("4000000050d506482dbdf024", "2012-12-22T01:00:46.767Z");
            put("@4000000050d506482dbdf024", "2012-12-22T01:00:46.767Z");

        }};
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]", failtagList);
        subject.acceptFilterConfig("TAI64N", loc, tz);
        for (Map.Entry<String, String> entry : testElements.entrySet()) {
            applyString(subject, entry.getKey(), entry.getValue());
        }
    }

    @Test
    public void testUnixStrings() throws Exception {
        Map<String, String> testElements = new HashMap<String, String>() {{
            put("0", "1970-01-01T00:00:00.000Z");
            put("1000000000", "2001-09-09T01:46:40.000Z");
            put("1478207457", "2016-11-03T21:10:57.000Z");
        }};
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]", failtagList);
        subject.acceptFilterConfig("UNIX", loc, tz);
        for (Map.Entry<String, String> entry : testElements.entrySet()) {
            applyString(subject, entry.getKey(), entry.getValue());
        }
    }
    @Test
    public void testUnixInts() throws Exception {
        Map<Integer, String> testElements = new HashMap<Integer, String>() {{
            put(0, "1970-01-01T00:00:00.000Z");
            put(1000000000, "2001-09-09T01:46:40.000Z");
            put(1478207457, "2016-11-03T21:10:57.000Z");
            put(456, "1970-01-01T00:07:36.000Z");
        }};
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]", failtagList);
        subject.acceptFilterConfig("UNIX", loc, tz);
        for (Map.Entry<Integer, String> entry : testElements.entrySet()) {
            applyInt(subject, entry.getKey(), entry.getValue());
        }
    }

    @Test
    public void testUnixLongs() throws Exception {
        Map<Long, String> testElements = new HashMap<Long, String>() {{
            put(0L, "1970-01-01T00:00:00.000Z");
            put(1000000000L, "2001-09-09T01:46:40.000Z");
            put(1478207457L, "2016-11-03T21:10:57.000Z");
            put(456L, "1970-01-01T00:07:36.000Z");
        }};
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]", failtagList);
        subject.acceptFilterConfig("UNIX", loc, tz);
        for (Map.Entry<Long, String> entry : testElements.entrySet()) {
            applyLong(subject, entry.getKey(), entry.getValue());
        }
    }

    @Test
    public void testUnixMillisLong() throws Exception {
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]", failtagList);
        subject.acceptFilterConfig("UNIX", loc, tz);
        subject.acceptFilterConfig("UNIX_MS", loc, tz);
        applyLong(subject, 1000000000123L, "2001-09-09T01:46:40.123Z");
    }

    @Test
    public void testUnixDouble() throws Exception {
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]", failtagList);
        subject.acceptFilterConfig("UNIX", loc, tz);
        applyDouble(subject, 1478207457.456D, "2016-11-03T21:10:57.456Z");
    }

    private void applyString(DateFilter subject, String supplied, String expected) {
        Event event = new Event();
        event.setField("[happened_at]", supplied);
        ParseExecutionResult code = subject.executeParsers(event);
        commonAssertions(event, code, expected);
    }

    private void applyInt(DateFilter subject, Integer supplied, String expected) {
        Event event = new Event();
        event.setField("[happened_at]", supplied);
        ParseExecutionResult code = subject.executeParsers(event);
        commonAssertions(event, code, expected);
    }

    private void applyLong(DateFilter subject, Long supplied, String expected) {
        Event event = new Event();
        event.setField("[happened_at]", supplied);
        ParseExecutionResult code = subject.executeParsers(event);
        commonAssertions(event, code, expected);
    }

    private void applyDouble(DateFilter subject, Double supplied, String expected) {
        Event event = new Event();
        event.setField("[happened_at]", supplied);
        ParseExecutionResult code = subject.executeParsers(event);
        commonAssertions(event, code, expected);
    }

    private void commonAssertions(Event event, ParseExecutionResult code, String expected) {
        Assert.assertSame(ParseExecutionResult.SUCCESS, code);
        String actual = ((Timestamp) event.getField("[result_ts]")).toIso8601();
        Assert.assertTrue(String.format("Unequal - expected: %s, actual: %s", expected, actual), expected.equals(actual));
    }
}

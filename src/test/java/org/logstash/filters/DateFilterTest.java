package org.logstash.filters;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.logstash.Event;
import org.logstash.Timestamp;

public class DateFilterTest {
    private List<String> failtagList = Collections
            .singletonList("_date_parse_fail");
    private String tz = "UTC";
    private String loc = "en";

    @Test
    public void testIsoStrings() throws Exception {

        Map<String, List<String>> testElements = new HashMap<String, List<String>>() {
            {
                put("2001-01-01T00:00:00-0800",
                        Arrays.asList("2001-01-01T08:00:00.000Z", null));
                put("1974-03-02T04:09:09-0800",
                        Arrays.asList("1974-03-02T12:09:09.000Z", null));
                put("2010-05-03 08:18:18+00:00",
                        Arrays.asList("2010-05-03T08:18:18.000Z", null));
                put("2004-07-04T12:27:27-00:00",
                        Arrays.asList("2004-07-04T12:27:27.000Z", null));
                put("2001-09-05 16:36:36+0000",
                        Arrays.asList("2001-09-05T16:36:36.000Z", null));
                put("2001-11-06T20:45:45-0000",
                        Arrays.asList("2001-11-06T20:45:45.000Z", null));
                put("2001-12-07T23:54:54Z",
                        Arrays.asList("2001-12-07T23:54:54.000Z", null));
            }
        };
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]",
                null, failtagList);
        subject.acceptFilterConfig("ISO8601", loc, tz);
        for (Map.Entry<String, List<String>> entry : testElements.entrySet()) {
            applyString(subject, entry.getKey(), entry.getValue().get(0),
                    entry.getValue().get(1));
        }
    }

    @Test
    public void testPatternStringsInterpolateTzNoYear() throws Exception {
        Map<String, List<String>> testElements = new HashMap<String, List<String>>() {
            {
                put("Mar 27 01:59:59.999",
                        Arrays.asList("2017-03-26T23:59:59.999Z",
                                "2017-03-26T23:59:59.999000000Z"));
                // put("Mar 27 02:00:01.000", "2016-03-27T01:00:01.000Z"); //
                // this should and does fail, the time does not exist
                put("Mar 27 03:00:01.000",
                        Arrays.asList("2017-03-27T01:00:01.000Z",
                                "2017-03-27T01:00:01.000000000Z")); // after
                // CET
                // to
                // CEST
                // change
                // at
                // 02:00
            }
        };
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]",
                "[nano_ts]", failtagList);
        subject.acceptFilterConfig("MMM dd HH:mm:ss.SSS", loc, "%{mytz}");
        for (Map.Entry<String, List<String>> entry : testElements.entrySet()) {
            applyStringTz(subject, entry.getKey(), entry.getValue().get(0),
                    entry.getValue().get(1), "CET");
        }
    }

    @Test
    public void testIsoStringsInterpolateTz() throws Exception {
        Map<String, List<String>> testElements = new HashMap<String, List<String>>() {
            {
                put("2001-01-01T00:00:00",
                        Arrays.asList("2001-01-01T04:00:00.000Z",
                                "2001-01-01T04:00:00.000000000Z"));
                put("1974-03-02T04:09:09",
                        Arrays.asList("1974-03-02T08:09:09.000Z",
                                "1974-03-02T08:09:09.000000000Z"));
                put("2006-01-01T00:00:00",
                        Arrays.asList("2006-01-01T04:00:00.000Z",
                                "2006-01-01T04:00:00.000000000Z"));
                // TIL Venezuela changed from -4:00 to -4:30 in late 2007 and
                // Joda 2.8.2 knows about this.
                put("2008-01-01T00:00:00",
                        Arrays.asList("2008-01-01T04:30:00.000Z",
                                "2008-01-01T04:30:00.000000000Z"));
                // TIL Venezuela changed from -4:30 to -4:00 on Sunday, 1 May
                // 2016 but Joda 2.8.2 does not know about this.
                put("2016-05-01 08:18:18.123",
                        Arrays.asList("2016-05-01T12:18:18.123Z",
                                "2016-05-01T12:18:18.123000000Z")); // "2016-05-01T12:18:18.123Z"
            }
        };
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]",
                "[nano_ts]", failtagList);
        subject.acceptFilterConfig("ISO8601", loc, "%{mytz}");
        for (Map.Entry<String, List<String>> entry : testElements.entrySet()) {
            applyStringTz(subject, entry.getKey(), entry.getValue().get(0),
                    entry.getValue().get(1), "America/Caracas");
        }
    }

    @Test
    public void testTai64Strings() throws Exception {
        Map<String, List<String>> testElements = new HashMap<String, List<String>>() {
            {
                put("4000000050d506482dbdf024",
                        Arrays.asList("2012-12-22T01:00:46.767Z",
                                "2012-12-22T01:00:46.767422500Z"));
                put("@4000000050d506482dbdf024",
                        Arrays.asList("2012-12-22T01:00:46.767Z",
                                "2012-12-22T01:00:46.767422500Z"));

            }
        };
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]",
                "[nano_ts]", failtagList);
        subject.acceptFilterConfig("TAI64N", loc, tz);
        for (Map.Entry<String, List<String>> entry : testElements.entrySet()) {
            applyString(subject, entry.getKey(), entry.getValue().get(0),
                    entry.getValue().get(1));
        }
    }

    @Test
    public void testUnixStrings() throws Exception {
        Map<String, List<String>> testElements = new HashMap<String, List<String>>() {
            {
                put("0", Arrays.asList("1970-01-01T00:00:00.000Z", null));
                put("1000000000",
                        Arrays.asList("2001-09-09T01:46:40.000Z", null));
                put("1478207457",
                        Arrays.asList("2016-11-03T21:10:57.000Z", null));
            }
        };
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]", "",
                failtagList);
        subject.acceptFilterConfig("UNIX", loc, tz);
        for (Map.Entry<String, List<String>> entry : testElements.entrySet()) {
            applyString(subject, entry.getKey(), entry.getValue().get(0),
                    entry.getValue().get(1));
        }
    }

    @Test
    public void testUnixInts() throws Exception {
        Map<Integer, List<String>> testElements = new HashMap<Integer, List<String>>() {
            {
                put(0, Arrays.asList("1970-01-01T00:00:00.000Z",
                        "1970-01-01T00:00:00.000000000Z"));
                put(1000000000, Arrays.asList("2001-09-09T01:46:40.000Z",
                        "2001-09-09T01:46:40.000000000Z"));
                put(1478207457, Arrays.asList("2016-11-03T21:10:57.000Z",
                        "2016-11-03T21:10:57.000000000Z"));
                put(456, Arrays.asList("1970-01-01T00:07:36.000Z",
                        "1970-01-01T00:07:36.000000000Z"));
            }
        };
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]",
                "[nano_ts]", failtagList);
        subject.acceptFilterConfig("UNIX", loc, tz);
        for (Map.Entry<Integer, List<String>> entry : testElements.entrySet()) {
            applyInt(subject, entry.getKey(), entry.getValue().get(0),
                    entry.getValue().get(1));
        }
    }

    @Test
    public void testUnixLongs() throws Exception {
        Map<Long, List<String>> testElements = new HashMap<Long, List<String>>() {
            {
                put(0L, Arrays.asList("1970-01-01T00:00:00.000Z",
                        "1970-01-01T00:00:00.000000000Z"));
                put(1000000000L, Arrays.asList("2001-09-09T01:46:40.000Z",
                        "2001-09-09T01:46:40.000000000Z"));
                put(1478207457L, Arrays.asList("2016-11-03T21:10:57.000Z",
                        "2016-11-03T21:10:57.000000000Z"));
                put(456L, Arrays.asList("1970-01-01T00:07:36.000Z",
                        "1970-01-01T00:07:36.000000000Z"));
            }
        };
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]",
                "[nano_ts]", failtagList);
        subject.acceptFilterConfig("UNIX", loc, tz);
        for (Map.Entry<Long, List<String>> entry : testElements.entrySet()) {
            applyLong(subject, entry.getKey(), entry.getValue().get(0),
                    entry.getValue().get(1));
        }
    }

    @Test
    public void testUnixMillisLong() throws Exception {
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]",
                "[nano_ts]", failtagList);
        subject.acceptFilterConfig("UNIX", loc, tz);
        subject.acceptFilterConfig("UNIX_MS", loc, tz);
        applyLong(subject, 1000000000123L, "2001-09-09T01:46:40.123Z",
                "2001-09-09T01:46:40.123000000Z");
    }

    @Test
    public void testUnixDouble() throws Exception {
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]",
                "[nano_ts]", failtagList);
        subject.acceptFilterConfig("UNIX", loc, tz);
        applyDouble(subject, 1478207457.456D, "2016-11-03T21:10:57.456Z",
                "2016-11-03T21:10:57.456000000Z");
    }

    @Test
    public void testCancelledEvent() throws Exception {
        DateFilter subject = new DateFilter("[happened_at]", "[result_ts]",
                "[nano_ts]", failtagList);
        subject.acceptFilterConfig("UNIX", loc, tz);

        Event event = new Event();
        event.cancel();
        event.setField("[happened_at]", 1478207457.456D);

        ParseExecutionResult code = subject.executeParsers(event);
        Assert.assertSame(ParseExecutionResult.IGNORED, code);
        Assert.assertNull(event.getField("[result_ts]"));
    }

    private void applyString(DateFilter subject, String supplied,
            String expected, String expectedNanoSec) {
        Event event = new Event();
        event.setField("[happened_at]", supplied);
        ParseExecutionResult code = subject.executeParsers(event);
        commonAssertions(event, code, expected, expectedNanoSec);
    }

    private void applyStringTz(DateFilter subject, String supplied,
            String expected, String expectedNanoSec, String tz) {
        Event event = new Event();
        event.setField("[happened_at]", supplied);
        event.setField("mytz", tz);
        ParseExecutionResult code = subject.executeParsers(event);
        commonAssertions(event, code, expected, expectedNanoSec);
    }

    private void applyInt(DateFilter subject, Integer supplied, String expected,
            String expectedNanoSec) {
        Event event = new Event();
        event.setField("[happened_at]", supplied);
        ParseExecutionResult code = subject.executeParsers(event);
        commonAssertions(event, code, expected, expectedNanoSec);
    }

    private void applyLong(DateFilter subject, Long supplied, String expected,
            String expectedNanoSec) {
        Event event = new Event();
        event.setField("[happened_at]", supplied);
        ParseExecutionResult code = subject.executeParsers(event);
        commonAssertions(event, code, expected, expectedNanoSec);
    }

    private void applyDouble(DateFilter subject, Double supplied,
            String expected, String expectedNanoSec) {
        Event event = new Event();
        event.setField("[happened_at]", supplied);
        ParseExecutionResult code = subject.executeParsers(event);
        commonAssertions(event, code, expected, expectedNanoSec);
    }

    private void commonAssertions(Event event, ParseExecutionResult code,
            String expected, String expectedNanoSec) {
        Assert.assertSame(ParseExecutionResult.SUCCESS, code);
        String actual = ((Timestamp) event.getField("[result_ts]")).toIso8601();
        Assert.assertTrue(String.format("Unequal - expected: %s, actual: %s",
                expected, actual), expected.equals(actual));
        String nanoSec = (String) event.getField("[nano_ts]");
        if (expectedNanoSec == null) {
            Assert.assertNull(nanoSec);
        } else {
            Assert.assertTrue(
                    "Expected nano sec " + expectedNanoSec
                            + " is not equal to actual " + nanoSec,
                    expectedNanoSec.equals(nanoSec));
        }
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.logstash.filters.parser;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;

public class Jsr310Parser implements TimestampParser {
    private final String timezone;
    private final Locale locale;
    private final boolean hasYear;
    private final boolean hasZone;
    private final DateTimeFormatter parser;

    public Jsr310Parser(String pattern, Locale locale, String timezone) {
        this.timezone = (timezone == null ? ZoneId.systemDefault().getId()
                : timezone);
        this.locale = (locale == null ? Locale.ENGLISH : locale);

        hasYear = (pattern.contains("Y") || pattern.contains("y")
                || pattern.contains("u"));
        hasZone = (pattern.contains("V") || pattern.contains("z")
                || pattern.contains("O") || pattern.contains("x")
                || pattern.contains("X") || pattern.contains("Z"));

        if (hasYear) {
            parser = new DateTimeFormatterBuilder().appendPattern(pattern)
                    .toFormatter(this.locale);
        } else {
            parser = new DateTimeFormatterBuilder().appendPattern(pattern)
                    .parseDefaulting(ChronoField.YEAR_OF_ERA,
                            Year.parse("2017").getValue())
                    .toFormatter(this.locale);
        }
    }

    @Override
    public Instant parse(String value) {
        return this.parseWithTimeZone(value, timezone);
    }

    @Override
    public Instant parse(Long value) {
        throw new IllegalArgumentException(
                "Expected a string value, but got a long (" + value
                        + "). Cannot parse date.");
    }

    @Override
    public Instant parse(Double value) {
        throw new IllegalArgumentException(
                "Expected a string value, but got a double (" + value
                        + "). Cannot parse date.");
    }

    @Override
    public Instant parse(BigDecimal value) {
        throw new IllegalArgumentException(
                "Expected a string value, but got a bigdecimal (" + value
                        + "). Cannot parse date.");
    }

    @Override
    public Instant parseWithTimeZone(String value, String timezone) {
        if (hasZone) {
            ZonedDateTime dt = ZonedDateTime.from(parser.parse(value));
            if (!hasYear) {
                return dt.with(ChronoField.YEAR_OF_ERA,
                        guessYear(dt.get(ChronoField.YEAR_OF_ERA),
                                dt.get(ChronoField.MONTH_OF_YEAR),
                                dt.getZone()))
                        .toInstant();
            } else {
                return dt.toInstant();
            }
        } else {
            LocalDateTime dt = LocalDateTime.from(parser.parse(value));
            ZoneId zone = ZoneId.of(timezone);
            ZonedDateTime zonedDateTime = ZonedDateTime.of(dt, zone);
            if (!hasYear) {
                return zonedDateTime.with(ChronoField.YEAR_OF_ERA,
                        guessYear(Year.now(zone).getValue(),
                                dt.get(ChronoField.MONTH_OF_YEAR), zone))
                        .toInstant();
            } else {
                return zonedDateTime.toInstant();
            }
        }
    }

    private int guessYear(int year, int month, ZoneId zone) {
        // if we get here, we need to do some special handling at the time each
        // event is handled
        // because things like the current year could be different, etc.
        int currentMonth = ZonedDateTime.now(zone).getMonthValue();
        if (month == 12 && currentMonth == 1) {
            // Now is January, event is December. Assume it's from last year.
            return year - 1;
        } else if (month == 1 && currentMonth == 12) {
            // Now is December, event is January. Assume it's from next year.
            return year + 1;
        } else {
            // Otherwise, assume it's from this year.
            return year;
        }
    }
}

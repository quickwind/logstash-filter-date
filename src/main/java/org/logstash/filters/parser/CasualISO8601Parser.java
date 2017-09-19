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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;

/**
 * Created by jls on 11/2/16.
 */
public class CasualISO8601Parser implements TimestampParser {
    private DateTimeFormatter zonedDTParser = new DateTimeFormatterBuilder()
            .optionalStart()
            .appendPattern(
                    "[yyyyMMdd][yyyy-MM-dd][yyyy-DDD]['T'[HHmmss][HHmm][HH:mm:ss][HH:mm][.SSSSSSSSS][.SSSSSS][.SSS][.SS][.S]][OOOO][O][z][XXXXX][XXXX]['['VV']']")
            .optionalEnd().optionalStart().optionalStart()
            .appendPattern(
                    "[yyyyMMdd][yyyy-MM-dd][yyyy-DDD][ [HHmmss][HHmm][HH:mm:ss][HH:mm][.SSSSSSSSS][.SSSSSS][.SSS][.SS][.S]][OOOO][O][z][XXXXX][XXXX]['['VV']']")
            .optionalEnd().optionalStart().optionalStart()
            .appendPattern(
                    "[yyyyMMdd][yyyy-MM-dd][yyyy-DDD]['T'[HHmmss][HHmm][HH:mm:ss][HH:mm][,SSSSSSSSS][,SSSSSS][,SSS][,SS][,S]][OOOO][O][z][XXXXX][XXXX]['['VV']']")
            .optionalEnd().optionalStart()
            .appendPattern(
                    "[yyyyMMdd][yyyy-MM-dd][yyyy-DDD][ [HHmmss][HHmm][HH:mm:ss][HH:mm][,SSSSSSSSS][,SSSSSS][,SSS][,SS][,S]][OOOO][O][z][XXXXX][XXXX]['['VV']']")
            .optionalEnd().toFormatter(Locale.ENGLISH);
    private DateTimeFormatter localDTParser = new DateTimeFormatterBuilder()
            .optionalStart()
            .appendPattern(
                    "[yyyyMMdd][yyyy-MM-dd][yyyy-DDD]['T'[HHmmss][HHmm][HH:mm:ss][HH:mm][.SSSSSSSSS][.SSSSSS][.SSS][.SS][.S]]")
            .optionalEnd().optionalStart().optionalStart()
            .appendPattern(
                    "[yyyyMMdd][yyyy-MM-dd][yyyy-DDD][ [HHmmss][HHmm][HH:mm:ss][HH:mm][.SSSSSSSSS][.SSSSSS][.SSS][.SS][.S]]")
            .optionalEnd().optionalStart().optionalStart()
            .appendPattern(
                    "[yyyyMMdd][yyyy-MM-dd][yyyy-DDD]['T'[HHmmss][HHmm][HH:mm:ss][HH:mm][,SSSSSSSSS][,SSSSSS][,SSS][,SS][,S]]")
            .optionalEnd().optionalStart()
            .appendPattern(
                    "[yyyyMMdd][yyyy-MM-dd][yyyy-DDD][ [HHmmss][HHmm][HH:mm:ss][HH:mm][,SSSSSSSSS][,SSSSSS][,SSS][,SS][,S]]")
            .optionalEnd().toFormatter(Locale.ENGLISH);
    private String timezone = null;

    public CasualISO8601Parser(String timezone) {
        this.timezone = timezone;
    }

    @Override
    public Instant parse(String value) {
        return this.parseWithTimeZone(value, this.timezone);
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
        try {
            ZonedDateTime dtDateTime = ZonedDateTime
                    .from(zonedDTParser.parse(value));
            return dtDateTime.toInstant();
        } catch (Exception ignore) {
            // Continue to parse...
        }

        LocalDateTime dtDateTime = LocalDateTime
                .from(localDTParser.parse(value));
        if (timezone != null) {
            return dtDateTime.atZone(ZoneId.of(timezone)).toInstant();
        } else {
            return dtDateTime.atZone(ZoneId.systemDefault()).toInstant();
        }
    }

}

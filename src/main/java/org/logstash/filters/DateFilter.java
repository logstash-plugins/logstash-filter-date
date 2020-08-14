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

package org.logstash.filters;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.Instant;
import org.logstash.Event;
import org.logstash.ext.JrubyEventExtLibrary.RubyEvent;
import org.logstash.filters.parser.CasualISO8601Parser;
import org.logstash.filters.parser.JodaParser;
import org.logstash.filters.parser.TimestampParser;
import org.logstash.filters.parser.TimestampParserFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DateFilter {
  private static Logger logger = LogManager.getLogger();
  private final String sourceField;
  private final String[] tagOnFailure;
  private RubyResultHandler successHandler;
  private RubyResultHandler failureHandler;
  private final List<ParserExecutor> executors = new ArrayList<>();
  private final ResultSetter setter;

  public interface RubyResultHandler {
    void handle(RubyEvent event);
  }

  public DateFilter(String sourceField, String targetField, List<String> tagOnFailure, RubyResultHandler successHandler, RubyResultHandler failureHandler) {
    this(sourceField, targetField, tagOnFailure);
    this.successHandler = successHandler;
    this.failureHandler = failureHandler;
  }

  public DateFilter(String sourceField, String targetField, List<String> tagOnFailure) {
    this.sourceField = sourceField;
    this.tagOnFailure = tagOnFailure.toArray(new String[0]);
    if (targetField.equals("@timestamp")) {
      this.setter = new TimestampSetter();
    } else {
      this.setter = new FieldSetter(targetField);
    }
  }

  public void acceptFilterConfig(String format, String locale, String timezone) {
    TimestampParser parser = TimestampParserFactory.makeParser(format, locale, timezone);
    logger.debug("Date filter with format={}, locale={}, timezone={} built as {}", format, locale, timezone, parser.getClass().getName());
    if (parser instanceof JodaParser || parser instanceof CasualISO8601Parser) {
      executors.add(new TextParserExecutor(parser, timezone, format));
    } else {
      executors.add(new NumericParserExecutor(parser, format));
    }
  }

 public List<RubyEvent> receive(List<RubyEvent> rubyEvents) {
    for (RubyEvent rubyEvent : rubyEvents) {
      Event event = rubyEvent.getEvent();

      switch (executeParsers(event)) {
        case FIELD_VALUE_IS_NULL_OR_FIELD_NOT_PRESENT:
        case IGNORED:
          continue;
        case SUCCESS:
          if (successHandler != null) {
            successHandler.handle(rubyEvent);
          }
          break;
        case FAIL: // fall through
        default:
          for (String t : tagOnFailure) {
            event.tag(t);
          }
          if (failureHandler != null) {
            failureHandler.handle(rubyEvent);
          }
      }
    }
    return rubyEvents;
  }

  public ParseExecutionResult executeParsers(Event event) {
    Object input = event.getField(sourceField);
    if (event.isCancelled()) { return ParseExecutionResult.IGNORED; }
    if (input == null) { return ParseExecutionResult.FIELD_VALUE_IS_NULL_OR_FIELD_NOT_PRESENT; }

    Throwable lastException = null;
    for (ParserExecutor executor : executors) {
      try {
        Instant instant = executor.execute(input, event);
        setter.set(event, instant);
        return ParseExecutionResult.SUCCESS;
      } catch (IllegalArgumentException | IOException e) {
        // TODO(sissel): Use structured logging.
        logger.debug("Date parse pattern '{}' failed on field '{}' with value '{}'. {}", executor.toString(), sourceField, input, e);
        // do nothing, try next ParserExecutor
      }
    }

    return ParseExecutionResult.FAIL;
  }
}

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
  private RubySuccessHandler successHandler;
  private final List<ParserExecutor> executors = new ArrayList<>();
  private final ResultSetter setter;

  public interface RubySuccessHandler {
    void success(RubyEvent event);
  }

  public DateFilter(String sourceField, String targetField, List<String> tagOnFailure, RubySuccessHandler successHandler) {
    this(sourceField, targetField, tagOnFailure);
    this.successHandler = successHandler;
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
      executors.add(new TextParserExecutor(parser, timezone));
    } else {
      executors.add(new NumericParserExecutor(parser));
    }
  }

//  public void register() {
//    // Nothing to do.
//  }

  //public Event[] receive(List<org.logstash.ext.JrubyEventExtLibrary.RubyEvent> rubyEvents) {

  public List<RubyEvent> receive(List<RubyEvent> rubyEvents) {
    for (RubyEvent rubyEvent : rubyEvents) {
      Event event = rubyEvent.getEvent();
      // XXX: Check for cast failures

      ParseExecutionResult code = executeParsers(event);
      if (ParseExecutionResult.NO_INPUT_FOUND == code) {
        continue;
      } else if (ParseExecutionResult.SUCCESS == code) {
        if (successHandler != null) {
          successHandler.success(rubyEvent);
        }
      } else {
        for (String t : tagOnFailure) {
          event.tag(t);
        }
      }
    }
    return rubyEvents;
  }

  public ParseExecutionResult executeParsers(Event event) {
    Object input = event.getField(sourceField);
    if (input == null) {
      return ParseExecutionResult.NO_INPUT_FOUND;
    }
    for (ParserExecutor executor : executors) {
      try {
        Instant instant = executor.execute(input, event);
        setter.set(event, instant);
        return ParseExecutionResult.SUCCESS;
      } catch (IllegalArgumentException | IOException e) {
        // do nothing, try next ParserExecutor
      }
    }
    return ParseExecutionResult.FAIL;
  }
}

/* * Licensed to Elasticsearch under one or more contributor
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

import org.joda.time.Instant;
import org.logstash.Event;
import org.logstash.Timestamp;
import org.logstash.ext.JrubyEventExtLibrary;
import org.logstash.ext.JrubyEventExtLibrary.RubyEvent;
import org.logstash.filters.parser.CasualISO8601Parser;
import org.logstash.filters.parser.JodaParser;
import org.logstash.filters.parser.TimestampParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DateFilter {
  private final boolean dynamicTimeZone;
  private final String sourceField;
  private final TimestampParser[] parsers;
  private final String targetField;
  private final String[] tagOnFailure;
  private final RubySuccessHandler successHandler;
  private String timeZone;

  public interface RubySuccessHandler {
    void success(RubyEvent event);
  }

  public DateFilter(String sourceField, List<TimestampParser> parsers, String targetField, List<String> tagOnFailure, String timeZone, RubySuccessHandler successHandler) {
    this.sourceField = sourceField;
    this.parsers = parsers.toArray(new TimestampParser[0]);
    this.targetField = targetField;
    this.tagOnFailure = tagOnFailure.toArray(new String[0]);
    this.timeZone = timeZone;
    this.dynamicTimeZone = timeZone != null && timeZone.contains("%{");
    this.successHandler = successHandler;
  }

  public void register() {
    // Nothing to do.
  }


  //public Event[] receive(List<org.logstash.ext.JrubyEventExtLibrary.RubyEvent> rubyEvents) {
  public List<RubyEvent> receive(List<RubyEvent> rubyEvents) {
    for (RubyEvent rubyEvent : rubyEvents) {
      Event event = rubyEvent.getEvent();
      // XXX: Check for cast failures
      //System.out.printf("Event: %s\n", event.toString());
      //System.out.printf("Source: %s\n", sourceField);
      Object input = event.getField(sourceField);
      //System.out.printf("Parsing: %s\n", input);
      if (input == null) {
        continue;
      }
      boolean success = false;
      for (TimestampParser parser : parsers) {
        try {
          //System.out.printf(" --> Trying %s\n", parser);
          // XXX: I am not certain `input.toString()` is best, here. This allows non-string values
          // to be parsed, such as Doubles, Longs, etc.
          Instant instant;
          if (parser instanceof JodaParser || parser instanceof CasualISO8601Parser) {
            if (!(input instanceof String)) {
              throw new IllegalArgumentException("Cannot parse date for value of type " + input.getClass().getName());
            }

            if (dynamicTimeZone) {
              // event.sprintf here can throw IOException due to a field reference lookup failure.
              //System.out.printf(" WithTimeZone: %s => %s", timeZone, event.sprintf(timeZone));
              instant = parser.parseWithTimeZone(input.toString(), event.sprintf(timeZone));
            } else {
              instant = parser.parse((String) input);
            }
          } else {
            if (input instanceof String) {
              instant = parser.parse((String) input);
            } else if (input instanceof Long) {
              instant = parser.parse((Long) input);
            } else if (input instanceof Double) {
              instant = parser.parse((Double) input);
            } else {
              throw new IllegalArgumentException("Cannot parse date for value of type " + input.getClass().getName());
            }
          }

          if (targetField.equals("@timestamp")) {
            event.setTimestamp(new Timestamp(instant.getMillis()));
          } else {
            event.setField(targetField, new Timestamp(instant.getMillis()));
          }

          success = true;
          break;
        } catch (IllegalArgumentException|IOException e) {
          // XXX: Store the last exception
          //System.out.printf("Exception => %s\n", e);
        }
      }

      if (success) {
        if (successHandler != null) {
          successHandler.success(rubyEvent);
        }
      } else {
        for (String t : tagOnFailure) {
          event.tag(t);
        }
      }
    }

    // multi_filter api in Logstash::Filters needs us to return the events.
    return rubyEvents;
  }
}

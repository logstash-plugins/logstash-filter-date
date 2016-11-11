package org.logstash.filters;

import org.joda.time.Instant;
import org.logstash.Event;
import org.logstash.filters.parser.TimestampParser;

import java.io.IOException;

class DynamicTzInputHandler implements InputHandler {
  private TimestampParser parser;
  private String timeZone;

  public DynamicTzInputHandler(TimestampParser parser, String timeZone) {
    this.parser = parser;
    this.timeZone = timeZone;
  }

  public DynamicTzInputHandler(TimestampParser parser) {
    this.parser = parser;
  }

  public Instant handle(String input, Event event) throws IOException {
    return this.parser.parseWithTimeZone(input, event.sprintf(timeZone));
  }
}

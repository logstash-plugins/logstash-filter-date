package org.logstash.filters;

import org.joda.time.Instant;
import org.logstash.Event;
import org.logstash.filters.parser.TimestampParser;

import java.io.IOException;

class StringInputHandler implements InputHandler {
  private TimestampParser parser;

  public StringInputHandler(TimestampParser parser) {
    this.parser = parser;
  }

  public Instant handle(String input, Event event) throws IOException {
    return this.parser.parse(input);
  }
}

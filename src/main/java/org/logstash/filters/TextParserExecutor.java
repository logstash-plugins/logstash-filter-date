package org.logstash.filters;

import org.joda.time.Instant;
import org.logstash.Event;
import org.logstash.filters.parser.TimestampParser;

import java.io.IOException;

class TextParserExecutor implements ParserExecutor {
  private InputHandler handler;

  public TextParserExecutor(TimestampParser parser, String timeZone) {
    if (timeZone != null && timeZone.contains("%{")) {
      this.handler = new DynamicTzInputHandler(parser, timeZone);
    } else {
      this.handler = new StringInputHandler(parser);
    }
  }

  public Instant execute(Object input, Event event) throws IOException {
    if (!(input instanceof String)) {
      throw new IllegalArgumentException("Cannot parse date for value of type " + input.getClass().getName());
    }
    return this.execute((String) input, event);
  }

  private Instant execute(String input, Event event) throws IOException {
    return this.handler.handle(input, event);
  }
}

package org.logstash.filters;

import org.joda.time.Instant;
import org.logstash.Event;
import org.logstash.filters.parser.TimestampParser;

import java.io.IOException;

class NumericParserExecutor implements ParserExecutor {
  private TimestampParser parser;
  public NumericParserExecutor(TimestampParser parser) {
    this.parser = parser;
  }

  public Instant execute(Object input, Event event) throws IOException {
    if (input instanceof String) {
      return parser.parse((String) input);
    } else if (input instanceof Long) {
      return parser.parse((Long)input);
    } else if (input instanceof Integer) {
      return parser.parse(((Integer) input).longValue());
    } else if (input instanceof Double) {
      return parser.parse((Double) input);
    } else {
      throw new IllegalArgumentException("Cannot parse date for value of type " + input.getClass().getName());
    }
  }
}

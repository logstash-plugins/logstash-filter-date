module LogStash
  module Filters
    class BaseDateParser

      attr_reader :timezone

      def initialize(timezone=nil)
          @timezone = timezone
      end

      def configure_offset(parser)
        if timezone
          parser.withZone(DateTimeZone.forID(timezone))
        else
          parser.withOffsetParsed
        end
      end

    end
  end
end

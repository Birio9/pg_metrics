require "simplecov"
SimpleCov.start { add_filter "/test/" }
require "test/unit"

[File.dirname(__FILE__),
 File.join(File.dirname(__FILE__), "..", "lib")].each do |f|
  $LOAD_PATH.unshift(f)
end

require "pg_metrics"


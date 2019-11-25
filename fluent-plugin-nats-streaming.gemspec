$:.push File.expand_path("../lib", __FILE__)

Gem::Specification.new do |gem|
  gem.name        = "fluent-plugin-nats-streaming"
  gem.version     = "0.0.1"
  gem.authors     = ["hc"]
  gem.email       = ["hc.chien@pentium.network"]
  gem.homepage    = "https://github.com/hc-chien/fluent-plugin-nats-streaming.git"
  gem.summary     = %q{nats streaming plugin for fluentd, an event collector}
  gem.description = %q{nats streaming plugin for fluentd, an event collector}
  gem.license     = "Apache-2.0"

  gem.files         = `git ls-files`.split("\n")
  gem.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.require_paths = ["lib"]
  gem.required_ruby_version = ">= 2.4.0"

#  gem.add_dependency "fluentd", ">= 0.14.20", "< 2"
#  nats-0.11.
  gem.add_dependency "nats", '~> 0.11', ">= 0.11.0"
  gem.add_dependency "nats-streaming", '~> 0.2', ">= 0.2.2"

  gem.add_development_dependency "rake", '~> 0.9', ">= 0.9.2"
  gem.add_development_dependency "test-unit", '~> 0.3', "> 3.1"
end

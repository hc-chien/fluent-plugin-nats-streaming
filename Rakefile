#!/usr/bin/env rake
require "bundler/gem_tasks"

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.test_files  = Dir['test/plugin/*.rb']
  test.verbose = false
  test.warning = false
end

task :default => :test
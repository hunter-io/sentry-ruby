# frozen_string_literal: true

require "bundler/setup"
begin
  require "debug/prelude"
rescue LoadError
end

ENV["RAILS_ENV"] = "test"

# SolidQueue requires Rails::Engine, so we must boot a minimal Rails app
require "rails"
require "active_model/railtie"
require "active_job/railtie"

# Minimal Rails application for testing
class SentryTestApp < Rails::Application
  config.eager_load = false
  config.active_job.queue_adapter = :solid_queue
  config.hosts = nil
  config.secret_key_base = "test123"
end

require "solid_queue"
require "sentry-ruby"
require "sentry/test_helper"
require "logger"

require 'simplecov'

SimpleCov.start do
  project_name "sentry-solid_queue"
  root File.join(__FILE__, "../../../")
  coverage_dir File.join(__FILE__, "../../coverage")
end

if ENV["CI"]
  require 'simplecov-cobertura'
  SimpleCov.formatter = SimpleCov::Formatter::CoberturaFormatter
end

require "sentry-solid_queue"

# In production, the Railtie initializer prepends ActiveJobExtensions via
# ActiveSupport.on_load(:active_job). In tests we don't fully initialize the Rails
# app, so we prepend manually to test the module behavior directly.
ActiveJob::Base.prepend(Sentry::SolidQueue::ActiveJobExtensions)

DUMMY_DSN = 'http://12345:67890@sentry.localdomain/sentry/42'

RSpec.configure do |config|
  config.example_status_persistence_file_path = ".rspec_status"
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before :suite do
    puts "\n"
    puts "*" * 100
    puts "Running with SolidQueue #{::SolidQueue::VERSION}"
    puts "*" * 100
    puts "\n"
  end

  config.before :each do
    ENV.delete('SENTRY_DSN')
    ENV.delete('SENTRY_CURRENT_ENV')
    ENV.delete('SENTRY_ENVIRONMENT')
    ENV.delete('SENTRY_RELEASE')
  end

  config.include(Sentry::TestHelper)

  config.after :each do
    Sentry::SolidQueue.detach_event_handlers
    reset_sentry_globals!
  end
end

def perform_basic_setup
  Sentry.init do |config|
    config.dsn = DUMMY_DSN
    config.sdk_logger = ::Logger.new(nil)
    config.background_worker_threads = 0
    config.transport.transport_class = Sentry::DummyTransport

    yield config if block_given?
  end
end

# --- Test job classes ---

class HappyJob < ActiveJob::Base
  self.queue_adapter = :solid_queue

  def perform
    crumb = Sentry::Breadcrumb.new(message: "I'm happy!")
    Sentry.add_breadcrumb(crumb)
    Sentry.set_tags mood: 'happy'
    "happy"
  end
end

class SadJob < ActiveJob::Base
  self.queue_adapter = :solid_queue

  def perform
    crumb = Sentry::Breadcrumb.new(message: "I'm sad!")
    Sentry.add_breadcrumb(crumb)
    Sentry.set_tags mood: 'sad'
    raise "I'm sad!"
  end
end

class ReportingJob < ActiveJob::Base
  self.queue_adapter = :solid_queue

  def perform
    Sentry.capture_message("I have something to say!")
  end
end

class NonSolidQueueJob < ActiveJob::Base
  self.queue_adapter = :async

  def perform
    "not solid_queue"
  end
end

# Raising jobs that count their own executions. Both take the `perform_now`
# guard's early-return path — CountingSadJob when Sentry is uninitialized,
# CountingNonSolidQueueSadJob because its adapter isn't solid_queue — which is
# where a method-level rescue used to swallow the error and perform the job a
# second time on the same instance.
class CountingSadJob < ActiveJob::Base
  self.queue_adapter = :solid_queue

  class << self
    attr_accessor :runs
  end
  self.runs = 0

  def perform
    self.class.runs += 1
    raise "counted failure"
  end
end

class CountingNonSolidQueueSadJob < ActiveJob::Base
  self.queue_adapter = :async

  class << self
    attr_accessor :runs
  end
  self.runs = 0

  def perform
    self.class.runs += 1
    raise "counted failure"
  end
end

class RetryableJob < ActiveJob::Base
  self.queue_adapter = :solid_queue
  retry_on RuntimeError, wait: 0, attempts: 3

  def perform
    raise "retry me!"
  end
end

class DiscardableJob < ActiveJob::Base
  self.queue_adapter = :solid_queue
  discard_on RuntimeError

  def perform
    raise "discard me!"
  end
end

class JobWithArguments < ActiveJob::Base
  self.queue_adapter = :solid_queue

  def perform(name, count:)
    "#{name}: #{count}"
  end
end

class FailingJobWithArguments < ActiveJob::Base
  self.queue_adapter = :solid_queue

  def perform(name, count:)
    raise "failed with #{name}: #{count}"
  end
end

class SilentArgumentJob < ActiveJob::Base
  self.queue_adapter = :solid_queue
  self.log_arguments = false

  def perform(secret_token)
    "processed #{secret_token}"
  end
end

class CronJob < ActiveJob::Base
  self.queue_adapter = :solid_queue

  def perform
    "cron ran"
  end
end

class FailingCronJob < ActiveJob::Base
  self.queue_adapter = :solid_queue

  def perform
    raise "cron failed!"
  end
end

# A fake GlobalID-able object for testing argument serialization
class FakeRecord
  attr_reader :id

  def initialize(id)
    @id = id
  end

  def to_global_id
    OpenStruct.new(to_s: "gid://app/FakeRecord/#{@id}")
  end
end

# --- Helpers ---

# Simulate the full serialize → deserialize → perform_now cycle
# that a SolidQueue worker thread would execute.
# Goes through JSON round-trip to match production behavior
# (SolidQueue stores serialized data as JSON in the database).
def simulate_worker_perform(job_class, *args, **kwargs)
  job = job_class.new(*args, **kwargs)
  job_data = job.serialize

  # In production, SolidQueue stores the serialized hash as JSON in the DB.
  # JSON.generate → JSON.parse converts symbol keys to strings.
  job_data = JSON.parse(JSON.generate(job_data))

  new_job = job_class.new
  new_job.deserialize(job_data)
  new_job.perform_now
end

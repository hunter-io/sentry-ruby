# frozen_string_literal: true

require "spec_helper"

# Mock SolidQueue::RecurringTask for testing
module SolidQueue
  class RecurringTask
    attr_reader :class_name, :schedule, :key

    # Registry for find_by lookups in tests
    @registry = {}

    class << self
      attr_reader :registry

      def find_by(key:)
        @registry[key]
      end

      def register(task)
        @registry[task.key] = task
      end

      def clear_registry!
        @registry.clear
      end
    end

    def initialize(class_name:, schedule:, key:)
      @class_name = class_name
      @schedule = schedule
      @key = key
    end
  end
end

# Test job for recurring tasks
class RecurringTestJob < ActiveJob::Base
  self.queue_adapter = :solid_queue

  def perform
    "recurring"
  end
end

class AlreadyPatchedJob < ActiveJob::Base
  self.queue_adapter = :solid_queue
  include Sentry::Cron::MonitorCheckIns
  sentry_monitor_check_ins slug: "user_patched", monitor_config: Sentry::Cron::MonitorConfig.from_crontab("5 * * * *")

  def perform
    "already patched"
  end
end

class AnotherRecurringJob < ActiveJob::Base
  self.queue_adapter = :solid_queue

  def perform
    "another recurring"
  end
end

RSpec.describe Sentry::SolidQueue::RecurringTasks do
  before do
    perform_basic_setup { |config| config.traces_sample_rate = 1.0 }
    described_class.reset!
    ::SolidQueue::RecurringTask.clear_registry!
  end

  describe ".patch_task" do
    it "patches a job class with MonitorCheckIns" do
      task = ::SolidQueue::RecurringTask.new(
        class_name: "RecurringTestJob",
        schedule: "*/5 * * * *",
        key: "my_recurring_task"
      )

      described_class.patch_task(task)

      expect(RecurringTestJob.ancestors).to include(Sentry::Cron::MonitorCheckIns)
    end

    it "uses task key as monitor slug" do
      task = ::SolidQueue::RecurringTask.new(
        class_name: "AnotherRecurringJob",
        schedule: "0 * * * *",
        key: "hourly_task"
      )

      described_class.patch_task(task)

      # The slug should be stored in the class
      expect(AnotherRecurringJob.sentry_monitor_slug).to eq("hourly_task")
    end

    it "skips classes already patched by user and memoizes" do
      task = ::SolidQueue::RecurringTask.new(
        class_name: "AlreadyPatchedJob",
        schedule: "*/10 * * * *",
        key: "should_not_override"
      )

      # Should not override the user's slug
      described_class.patch_task(task)

      expect(AlreadyPatchedJob.sentry_monitor_slug).to eq("user_patched")
      expect(described_class.instance_variable_get(:@patched_classes)).to include("AlreadyPatchedJob")
    end

    it "handles invalid class names gracefully without memoizing" do
      task = ::SolidQueue::RecurringTask.new(
        class_name: "NonExistentClass",
        schedule: "* * * * *",
        key: "bad_task"
      )

      expect { described_class.patch_task(task) }.not_to raise_error
      expect(described_class.instance_variable_get(:@patched_classes)).not_to include("NonExistentClass")
    end

    it "retries patching when class was not loaded on first attempt" do
      task = ::SolidQueue::RecurringTask.new(
        class_name: "LateLoadedJob",
        schedule: "*/5 * * * *",
        key: "late_loaded"
      )

      # First attempt: class doesn't exist yet
      described_class.patch_task(task)
      expect(described_class.instance_variable_get(:@patched_classes)).not_to include("LateLoadedJob")

      # Class gets loaded later (e.g., after eager_load!)
      stub_const("LateLoadedJob", Class.new(ActiveJob::Base) {
        self.queue_adapter = :solid_queue
      })

      # Second attempt: class now exists, should be patched
      described_class.patch_task(task)
      expect(described_class.instance_variable_get(:@patched_classes)).to include("LateLoadedJob")
      expect(LateLoadedJob.ancestors).to include(Sentry::Cron::MonitorCheckIns)
    end

    it "caches patched classes (second call is no-op)" do
      task = ::SolidQueue::RecurringTask.new(
        class_name: "RecurringTestJob",
        schedule: "*/5 * * * *",
        key: "my_recurring_task"
      )

      described_class.patch_task(task)
      patched_count = described_class.instance_variable_get(:@patched_classes).size

      described_class.patch_task(task)
      expect(described_class.instance_variable_get(:@patched_classes).size).to eq(patched_count)
    end

    it "skips classes that are not ActiveJob::Base subclasses" do
      # Define a non-ActiveJob class
      stub_const("NotAJob", Class.new)

      task = ::SolidQueue::RecurringTask.new(
        class_name: "NotAJob",
        schedule: "* * * * *",
        key: "not_a_job_task"
      )

      described_class.patch_task(task)
      expect(NotAJob.ancestors).not_to include(Sentry::Cron::MonitorCheckIns)
    end

    it "skips tasks without a schedule and memoizes" do
      task = ::SolidQueue::RecurringTask.new(
        class_name: "RecurringTestJob",
        schedule: nil,
        key: "no_schedule"
      )

      described_class.reset!

      expect { described_class.patch_task(task) }.not_to raise_error
      expect(described_class.instance_variable_get(:@patched_classes)).to include("RecurringTestJob")
    end

    it "is safe under concurrent patch_task calls" do
      threads = 10.times.map do
        Thread.new do
          task = ::SolidQueue::RecurringTask.new(
            class_name: "RecurringTestJob",
            schedule: "*/5 * * * *",
            key: "concurrent_task"
          )
          described_class.patch_task(task)
        end
      end

      threads.each(&:join)

      expect(described_class.instance_variable_get(:@patched_classes)).to include("RecurringTestJob")
    end
  end

  describe ".resolve_task" do
    it "looks up a RecurringTask by key" do
      task = ::SolidQueue::RecurringTask.new(
        class_name: "RecurringTestJob",
        schedule: "*/5 * * * *",
        key: "my_task"
      )
      ::SolidQueue::RecurringTask.register(task)

      resolved = described_class.resolve_task("my_task")
      expect(resolved).to eq(task)
    end

    it "returns nil for unknown keys" do
      resolved = described_class.resolve_task("nonexistent_key")
      expect(resolved).to be_nil
    end
  end

  describe ".setup" do
    it "subscribes to enqueue_recurring_task.solid_queue and calls patch_task" do
      described_class.setup

      task = ::SolidQueue::RecurringTask.new(
        class_name: "RecurringTestJob",
        schedule: "*/5 * * * *",
        key: "setup_test_task"
      )

      ActiveSupport::Notifications.instrument("enqueue_recurring_task.solid_queue", task: task)

      expect(described_class.instance_variable_get(:@patched_classes)).to include("RecurringTestJob")
    end

    it "resolves string task keys to RecurringTask objects (solid_queue >= 1.3)" do
      described_class.setup

      task = ::SolidQueue::RecurringTask.new(
        class_name: "RecurringTestJob",
        schedule: "*/5 * * * *",
        key: "string_key_task"
      )
      ::SolidQueue::RecurringTask.register(task)

      ActiveSupport::Notifications.instrument("enqueue_recurring_task.solid_queue", task: "string_key_task")

      expect(described_class.instance_variable_get(:@patched_classes)).to include("RecurringTestJob")
    end

    it "skips string task keys that cannot be resolved" do
      described_class.setup

      expect {
        ActiveSupport::Notifications.instrument("enqueue_recurring_task.solid_queue", task: "unknown_key")
      }.not_to raise_error
    end

    it "skips events with no task payload" do
      described_class.setup

      expect {
        ActiveSupport::Notifications.instrument("enqueue_recurring_task.solid_queue", task: nil)
      }.not_to raise_error
    end
  end

  describe ".build_monitor_config" do
    it "builds config from standard cron expression" do
      config = described_class.build_monitor_config("*/5 * * * *")

      expect(config).to be_a(Sentry::Cron::MonitorConfig)
      expect(config.schedule).to be_a(Sentry::Cron::MonitorSchedule::Crontab)
      expect(config.schedule.value).to eq("*/5 * * * *")
    end

    it "extracts timezone from 6-part cron expression" do
      config = described_class.build_monitor_config("0 * * * * US/Eastern")

      expect(config).to be_a(Sentry::Cron::MonitorConfig)
      expect(config.schedule.value).to eq("0 * * * *")
      expect(config.timezone).to eq("US/Eastern")
    end

    it "handles cron with extra whitespace" do
      config = described_class.build_monitor_config("  0 * * * *  ")

      expect(config).to be_a(Sentry::Cron::MonitorConfig)
      expect(config.schedule).to be_a(Sentry::Cron::MonitorSchedule::Crontab)
    end
  end
end

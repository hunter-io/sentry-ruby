# frozen_string_literal: true

module Sentry
  module SolidQueue
    module RecurringTasks
      @patched_classes = Set.new
      @mutex = Mutex.new

      def self.reset!
        @mutex.synchronize { @patched_classes.clear }
      end

      def self.setup
        return unless defined?(::SolidQueue::RecurringTask)

        # Subscribe via tracked subscriber for test cleanup
        Sentry::SolidQueue.subscribe("enqueue_recurring_task.solid_queue") do |event|
          task = event.payload[:task]
          next unless task

          # In solid_queue >= 1.3, the payload :task is the string key, not the RecurringTask object
          task = resolve_task(task) if task.is_a?(String)
          next unless task

          patch_task(task)
        end
      end

      def self.resolve_task(key)
        ::SolidQueue::RecurringTask.find_by(key: key)
      rescue ActiveRecord::ActiveRecordError
        nil
      end

      def self.patch_task(task)
        klass_name = task.class_name

        @mutex.synchronize do
          return if @patched_classes.include?(klass_name)

          klass_const = klass_name.safe_constantize
          # Don't memoize if class can't be resolved — it may not be loaded yet
          return unless klass_const

          # Class is resolved; memoize to prevent repeated processing
          @patched_classes.add(klass_name)

          return unless klass_const < ActiveJob::Base

          # Only patch if not already patched by user
          return if klass_const.ancestors.include?(Sentry::Cron::MonitorCheckIns)

          schedule = task.schedule
          return unless schedule

          monitor_config = build_monitor_config(schedule)
          return unless monitor_config

          klass_const.include(Sentry::Cron::MonitorCheckIns)
          klass_const.sentry_monitor_check_ins(
            slug: task.key.to_s,
            monitor_config: monitor_config
          )

          Sentry.sdk_logger.info("Injected Sentry Crons monitor check-ins into #{klass_name}")
        end
      end

      # Extract timezone suffix from cron expression if present.
      # Matches sentry-sidekiq/lib/sentry/sidekiq/cron/helpers.rb
      def self.build_monitor_config(cron)
        cron_parts = cron.strip.split(" ")

        if cron_parts.length > 5
          timezone = cron_parts.pop
          cron_without_timezone = cron_parts.join(" ")
          Sentry::Cron::MonitorConfig.from_crontab(cron_without_timezone, timezone: timezone)
        else
          Sentry::Cron::MonitorConfig.from_crontab(cron)
        end
      end
    end
  end
end

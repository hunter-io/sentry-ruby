# frozen_string_literal: true

module Sentry
  module SolidQueue
    module ActiveJobExtensions
      OP_NAME = "queue.process"
      SPAN_ORIGIN = "auto.queue.solid_queue"
      SENTRY_TRACE_KEY = "_sentry"
      PROPAGATED_USER_FIELDS = %w[id email username].freeze

      # --- Server-side: wrap execution ---
      def perform_now
        return super unless Sentry.initialized? && using_solid_queue_adapter?

        job_executed = false

        # NOTE: the rescue below MUST stay scoped to this begin block. As a
        # method-level `rescue` it also covered the guard clause above, where
        # `job_executed` is still nil — so an exception raised by the guard's
        # own `super` was swallowed and the job was performed a second time on
        # the same instance (executions == 2, duplicated side effects, and the
        # *second* run's error propagating in place of the real one).
        begin
          # Worker threads (deserialized from queue): clone hub for thread isolation,
          # then operate directly on the scope (like Sidekiq's server middleware).
          # No with_scope — avoids double Scope#dup overhead.
          #
          # Inline perform_now (e.g., from a controller): use with_scope to
          # preserve the existing request scope/transaction.
          if @_solid_queue_worker_thread
            Sentry.clone_hub_to_current_thread
            scope = Sentry.get_current_scope
            perform_with_sentry(scope) { job_executed = true; super }
          else
            Sentry.with_scope do |scope|
              perform_with_sentry(scope) { job_executed = true; super }
            end
          end
        rescue => e
          # If the job already executed, the exception is a re-raised job error —
          # let it propagate. If not, Sentry setup failed before the job ran,
          # so fall back to running the job without instrumentation.
          raise if job_executed
          Sentry.sdk_logger.error("sentry-solid_queue failed to instrument job: #{e.message}") rescue nil
          super
        end
      end

      # --- Client-side: inject trace headers on enqueue ---
      def serialize
        result = super
        return result unless Sentry.initialized? && using_solid_queue_adapter?

        sentry_data = {}

        if Sentry.configuration.solid_queue.propagate_traces
          sentry_data["trace_propagation_headers"] = Sentry.get_trace_propagation_headers
        end

        # Only propagate user context when send_default_pii is enabled,
        # since this data is stored in the SolidQueue database (SQL).
        # Allowlist fields to avoid leaking sensitive data (ip_address, etc.).
        if Sentry.configuration.send_default_pii
          user = Sentry.get_current_scope.user
          filtered = user.select { |k, _| PROPAGATED_USER_FIELDS.include?(k.to_s) }
          sentry_data["user"] = filtered unless filtered.empty?
        end

        result[SENTRY_TRACE_KEY] = sentry_data if sentry_data.any?

        result
      end

      # --- Client-side: wrap enqueue with a span measuring actual I/O ---
      def enqueue(options = {})
        return super unless Sentry.initialized? && using_solid_queue_adapter?

        Sentry.with_child_span(op: "queue.publish", description: self.class.name) do |span|
          if span
            span.set_data(Sentry::Span::DataConventions::MESSAGING_MESSAGE_ID, job_id)
            span.set_data(Sentry::Span::DataConventions::MESSAGING_DESTINATION_NAME, queue_name)
          end
          super
        end
      end

      def deserialize(job_data)
        super
        return unless using_solid_queue_adapter?

        # Validate type to guard against malformed/tampered job data
        sentry_data = job_data[SENTRY_TRACE_KEY]
        @_sentry_trace_data = sentry_data if sentry_data.is_a?(Hash)
        # Mark that this job was deserialized from a queue (not inline perform_now).
        # Used to decide whether to clone the hub to isolate per worker thread.
        @_solid_queue_worker_thread = true
      end

      private

      def perform_with_sentry(scope)
        scope.set_transaction_name(self.class.name, source: :task)
        scope.set_tags(queue: queue_name, job_id: job_id)
        scope.set_contexts(solid_queue: sentry_base_context)

        # Restore user context from enqueue (only if it was propagated)
        if (sentry_data = @_sentry_trace_data)
          user = sentry_data["user"]
          scope.set_user(user) if user.is_a?(Hash) && user.any?
        end

        transaction = start_sentry_transaction(scope)
        if transaction
          scope.set_span(transaction)
          set_span_data(transaction)
        end

        begin
          result = yield
          finish_sentry_transaction(transaction, 200)
          result
        rescue Exception => e # rubocop:disable Lint/RescueException
          finish_sentry_transaction(transaction, 500)
          capture_exception(e)
          raise
        ensure
          scope.clear if @_solid_queue_worker_thread
        end
      end

      def using_solid_queue_adapter?
        self.class.queue_adapter_name == "solid_queue"
      end

      def start_sentry_transaction(scope)
        options = {
          name: scope.transaction_name,
          source: scope.transaction_source,
          op: OP_NAME,
          origin: SPAN_ORIGIN
        }

        # Always call continue_trace (handles nil headers gracefully),
        # matching the pattern in sentry-sidekiq's server middleware.
        headers = @_sentry_trace_data&.dig("trace_propagation_headers")
        transaction = Sentry.continue_trace(headers, **options)
        Sentry.start_transaction(transaction: transaction, **options)
      end

      def finish_sentry_transaction(transaction, status)
        return unless transaction

        transaction.set_http_status(status)
        transaction.finish
      end

      def set_span_data(span)
        span.set_data(Sentry::Span::DataConventions::MESSAGING_MESSAGE_ID, job_id)
        span.set_data(Sentry::Span::DataConventions::MESSAGING_DESTINATION_NAME, queue_name)
        span.set_data(Sentry::Span::DataConventions::MESSAGING_MESSAGE_RETRY_COUNT, executions) if executions > 0

        enqueued_time = parse_enqueued_time
        if enqueued_time
          latency_ms = ((Time.now.to_f - enqueued_time.to_f) * 1000).round
          span.set_data(Sentry::Span::DataConventions::MESSAGING_MESSAGE_RECEIVE_LATENCY, latency_ms)
        end
      end

      def parse_enqueued_time
        case enqueued_at
        when Time then enqueued_at
        when String then Time.iso8601(enqueued_at)
        end
      rescue ArgumentError
        nil
      end

      def capture_exception(exception)
        Sentry::SolidQueue.capture_exception(
          exception,
          contexts: { solid_queue: sentry_context },
          hint: { background: false }
        )
      end

      def sentry_base_context
        {
          active_job: self.class.name,
          job_id: job_id,
          provider_job_id: provider_job_id,
          queue: queue_name,
          scheduled_at: scheduled_at,
          executions: executions,
          locale: locale
        }.compact
      end

      def sentry_context
        ctx = sentry_base_context
        ctx[:arguments] = sentry_serialize_arguments(arguments) if self.class.log_arguments?
        ctx
      end

      SERIALIZE_MAX_DEPTH = 10

      # Safely serialize arguments for Sentry context.
      # Based on sentry-rails/lib/sentry/rails/active_job.rb with added depth limit.
      def sentry_serialize_arguments(argument, depth: 0)
        return "<deep>" if depth > SERIALIZE_MAX_DEPTH

        case argument
        when Range
          argument.to_s
        when Hash
          argument.transform_values { |v| sentry_serialize_arguments(v, depth: depth + 1) }
        when Array
          argument.map { |v| sentry_serialize_arguments(v, depth: depth + 1) }
        when ->(v) { v.respond_to?(:to_global_id) }
          argument.to_global_id.to_s rescue argument
        else
          argument
        end
      end
    end
  end
end

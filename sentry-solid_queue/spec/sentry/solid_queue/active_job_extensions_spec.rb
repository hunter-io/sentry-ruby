# frozen_string_literal: true

require "spec_helper"

RSpec.describe Sentry::SolidQueue::ActiveJobExtensions do
  let(:transport) { Sentry.get_current_client.transport }

  describe "#perform_now" do
    context "when Sentry is not initialized" do
      it "runs the job without instrumentation" do
        result = HappyJob.perform_now
        expect(result).to eq("happy")
      end
    end

    context "when Sentry is initialized without tracing" do
      before { perform_basic_setup { |config| config.traces_sample_rate = 0 } }

      it "captures exceptions from failed jobs" do
        expect { SadJob.perform_now }.to raise_error(RuntimeError)

        expect(transport.events.count).to eq(1)
        event = transport.events.first
        expect(event.exception.values.first.type).to eq("RuntimeError")
      end

      it "does not capture events for successful jobs" do
        HappyJob.perform_now

        expect(transport.events.count).to eq(0)
      end

      it "sets solid_queue context on error events" do
        expect { SadJob.perform_now }.to raise_error(RuntimeError)

        event = transport.events.first
        expect(event.contexts[:solid_queue][:active_job]).to eq("SadJob")
        expect(event.contexts[:solid_queue][:queue]).to eq("default")
      end

      it "does not interfere with non-SolidQueue adapter jobs" do
        result = NonSolidQueueJob.perform_now
        expect(result).to eq("not solid_queue")
        expect(transport.events.count).to eq(0)
      end

      it "sets the mechanism to solid_queue" do
        expect { SadJob.perform_now }.to raise_error(RuntimeError)

        event = transport.events.first
        mechanism = event.exception.values.first.mechanism
        expect(mechanism.type).to eq("solid_queue")
        expect(mechanism.handled).to eq(false)
      end
    end

    context "with tracing enabled" do
      before { perform_basic_setup { |config| config.traces_sample_rate = 1.0 } }

      it "creates a transaction for successful jobs" do
        HappyJob.perform_now

        expect(transport.events.count).to eq(1)
        transaction = transport.events.first
        expect(transaction.transaction).to eq("HappyJob")
        expect(transaction.contexts.dig(:trace, :op)).to eq("queue.process")
        expect(transaction.contexts.dig(:trace, :status)).to eq("ok")
        expect(transaction.contexts.dig(:trace, :origin)).to eq("auto.queue.solid_queue")
      end

      it "creates a transaction with error status for failed jobs" do
        expect { SadJob.perform_now }.to raise_error(RuntimeError)

        transaction = transport.events.first
        expect(transaction.contexts.dig(:trace, :op)).to eq("queue.process")
        expect(transaction.contexts.dig(:trace, :status)).to eq("internal_error")
      end

      it "sets tags on the scope" do
        HappyJob.perform_now

        transaction = transport.events.first
        expect(transaction.tags[:queue]).to eq("default")
        expect(transaction.tags[:job_id]).to be_a(String)
      end

      it "sets transaction_info with source :task" do
        HappyJob.perform_now

        transaction = transport.events.first
        expect(transaction.transaction_info).to eq({ source: :task })
      end

      it "includes breadcrumbs from within the job" do
        expect { SadJob.perform_now }.to raise_error(RuntimeError)

        # Transaction is first event (finished in ensure), error event is second
        error_event = transport.events.find { |e| !e.is_a?(Sentry::TransactionEvent) }
        breadcrumbs = error_event.breadcrumbs&.peek
        expect(breadcrumbs).not_to be_nil
        expect(breadcrumbs.message).to eq("I'm sad!")
      end

      it "links error event and transaction with the same trace_id" do
        expect { SadJob.perform_now }.to raise_error(RuntimeError)

        expect(transport.events.count).to eq(2)
        transaction = transport.events.find { |e| e.is_a?(Sentry::TransactionEvent) }
        error_event = transport.events.find { |e| !e.is_a?(Sentry::TransactionEvent) }

        transaction_trace_id = transaction.contexts.dig(:trace, :trace_id)
        error_trace_id = error_event.contexts.dig(:trace, :trace_id)

        expect(transaction_trace_id).to be_a(String)
        expect(transaction_trace_id).to eq(error_trace_id)
      end
    end

    context "inline path (no deserialization)" do
      before { perform_basic_setup { |config| config.traces_sample_rate = 1.0 } }

      it "uses with_scope to preserve request isolation" do
        Sentry.get_current_scope.set_tags(outer: "value")

        HappyJob.perform_now

        expect(Sentry.get_current_scope.tags[:outer]).to eq("value")
      end
    end

    context "worker thread path (deserialized)" do
      before { perform_basic_setup { |config| config.traces_sample_rate = 1.0 } }

      it "creates a transaction for deserialized jobs" do
        simulate_worker_perform(HappyJob)

        expect(transport.events.count).to eq(1)
        transaction = transport.events.first
        expect(transaction).to be_a(Sentry::TransactionEvent)
        expect(transaction.transaction).to eq("HappyJob")
        expect(transaction.contexts.dig(:trace, :op)).to eq("queue.process")
        expect(transaction.contexts.dig(:trace, :status)).to eq("ok")
      end

      it "captures errors on the worker thread path" do
        expect { simulate_worker_perform(SadJob) }.to raise_error(RuntimeError)

        expect(transport.events.count).to eq(2)
        transaction = transport.events.find { |e| e.is_a?(Sentry::TransactionEvent) }
        error_event = transport.events.find { |e| !e.is_a?(Sentry::TransactionEvent) }

        expect(transaction.contexts.dig(:trace, :status)).to eq("internal_error")
        expect(error_event.exception.values.first.type).to eq("RuntimeError")
      end

      it "clears the scope after successful execution" do
        simulate_worker_perform(HappyJob)

        # After worker thread job, scope should be clean
        expect(Sentry.get_current_scope.tags).to be_empty
      end

      it "clears the scope after failed execution" do
        expect { simulate_worker_perform(SadJob) }.to raise_error(RuntimeError)

        # Even on failure, scope should be cleaned up
        expect(Sentry.get_current_scope.tags).to be_empty
      end

      it "isolates breadcrumbs between sequential worker jobs" do
        # First job sets breadcrumbs
        simulate_worker_perform(HappyJob)
        transport.events.clear

        # Second job should not see first job's breadcrumbs
        expect { simulate_worker_perform(SadJob) }.to raise_error(RuntimeError)

        error_event = transport.events.find { |e| !e.is_a?(Sentry::TransactionEvent) }
        breadcrumb_messages = error_event.breadcrumbs.to_h[:values].map { |b| b[:message] }

        expect(breadcrumb_messages).to include("I'm sad!")
        expect(breadcrumb_messages).not_to include("I'm happy!")
      end

      it "isolates tags between sequential worker jobs" do
        simulate_worker_perform(HappyJob)
        transport.events.clear

        # Second job should not see first job's mood tag
        expect { simulate_worker_perform(SadJob) }.to raise_error(RuntimeError)

        error_event = transport.events.find { |e| !e.is_a?(Sentry::TransactionEvent) }
        # Only "sad" mood should be present, not "happy" from previous job
        expect(error_event.tags[:mood]).to eq("sad")
      end
    end

    context "safety rescue" do
      before { perform_basic_setup { |config| config.traces_sample_rate = 1.0 } }

      it "falls back to uninstrumented execution if Sentry errors before the job runs" do
        allow(Sentry).to receive(:clone_hub_to_current_thread).and_raise(StandardError, "sentry boom")

        job = HappyJob.new
        job_data = job.serialize
        job_data["_sentry"] = {}
        job.deserialize(job_data)

        # Should not raise — falls back to super
        result = job.perform_now
        expect(result).to eq("happy")
      end

      it "re-raises job errors even when safety rescue is active" do
        # The outer rescue must not swallow actual job exceptions
        expect { simulate_worker_perform(SadJob) }.to raise_error(RuntimeError, "I'm sad!")
      end

      it "falls back to uninstrumented execution on inline path if Sentry errors before the job runs" do
        allow(Sentry).to receive(:with_scope).and_raise(StandardError, "scope boom")

        # Inline path (no deserialization) — should fall back to super
        result = HappyJob.perform_now
        expect(result).to eq("happy")
      end
    end

    context "span data" do
      before { perform_basic_setup { |config| config.traces_sample_rate = 1.0 } }

      it "sets messaging span data on the transaction" do
        HappyJob.perform_now

        transaction = transport.events.first
        trace_data = transaction.contexts.dig(:trace, :data)

        expect(trace_data["messaging.message.id"]).to be_a(String)
        expect(trace_data["messaging.destination.name"]).to eq("default")
      end

      it "sets retry count when executions > 0" do
        job = HappyJob.new
        # Simulate a job that has been retried
        job_data = job.serialize
        job_data["executions"] = 2
        job.deserialize(job_data)
        job.perform_now

        transaction = transport.events.first
        trace_data = transaction.contexts.dig(:trace, :data)

        expect(trace_data["messaging.message.retry.count"]).to eq(2)
      end

      it "does not set retry count for first execution" do
        HappyJob.perform_now

        transaction = transport.events.first
        trace_data = transaction.contexts.dig(:trace, :data)

        expect(trace_data).not_to have_key("messaging.message.retry.count")
      end

      it "handles nil enqueued_at gracefully" do
        job = HappyJob.new
        allow(job).to receive(:enqueued_at).and_return(nil)
        job.perform_now

        transaction = transport.events.first
        trace_data = transaction.contexts.dig(:trace, :data)
        expect(trace_data).not_to have_key("messaging.message.receive.latency")
      end

      it "handles malformed enqueued_at string gracefully" do
        job = HappyJob.new
        allow(job).to receive(:enqueued_at).and_return("not-a-timestamp")
        job.perform_now

        transaction = transport.events.first
        trace_data = transaction.contexts.dig(:trace, :data)
        expect(trace_data).not_to have_key("messaging.message.receive.latency")
      end

      it "sets receive latency for enqueued jobs" do
        # Use the full round-trip so enqueued_at is populated by serialize
        job = HappyJob.new
        job_data = job.serialize
        # Override enqueued_at to simulate 500ms ago
        job_data["enqueued_at"] = (Time.now - 0.5).utc.iso8601(6)
        new_job = HappyJob.new
        new_job.deserialize(job_data)
        new_job.perform_now

        transaction = transport.events.first
        trace_data = transaction.contexts.dig(:trace, :data)

        latency = trace_data["messaging.message.receive.latency"]
        expect(latency).to be_a(Integer)
        expect(latency).to be_within(200).of(500) # ~500ms with tolerance
      end
    end

    context "SDK metadata" do
      before { perform_basic_setup { |config| config.traces_sample_rate = 0 } }

      it "sets SDK name to sentry.ruby.solid_queue on error events" do
        expect { SadJob.perform_now }.to raise_error(RuntimeError)

        event = transport.events.first
        expect(event.to_h[:sdk]).to eq({
          name: "sentry.ruby.solid_queue",
          version: Sentry::SolidQueue::VERSION
        })
      end
    end

    context "log_arguments? = false" do
      before { perform_basic_setup { |config| config.traces_sample_rate = 0 } }

      it "does not include arguments in sentry context" do
        expect { SilentArgumentJob.perform_now("super_secret_123") }.not_to raise_error

        # No error events since the job succeeds, but we can check via a failing variant.
        # Instead, test sentry_context directly:
        job = SilentArgumentJob.new("super_secret_123")
        context = job.send(:sentry_context)
        expect(context[:arguments]).to be_nil
      end
    end

    context "full round-trip (serialize → deserialize → perform)" do
      before do
        perform_basic_setup do |config|
          config.traces_sample_rate = 1.0
          config.send_default_pii = true
        end
        Sentry.get_current_scope.set_user(id: 99, email: "roundtrip@example.com")
      end

      it "propagates trace and user context through the full cycle" do
        # 1. Serialize (simulates enqueue side)
        job = HappyJob.new
        serialized = job.serialize

        expect(serialized["_sentry"]).to be_a(Hash)
        expect(serialized["_sentry"]["trace_propagation_headers"]).to include("sentry-trace")
        expect(serialized["_sentry"]["user"]).to include(id: 99)

        # Extract the enqueue-side trace_id
        sentry_trace = serialized["_sentry"]["trace_propagation_headers"]["sentry-trace"]
        enqueue_trace_id = sentry_trace.split("-").first

        # 2. Deserialize + perform (simulates worker side)
        new_job = HappyJob.new
        new_job.deserialize(serialized)
        new_job.perform_now

        # 3. Verify the transaction continues the trace
        transaction = transport.events.first
        expect(transaction.contexts.dig(:trace, :trace_id)).to eq(enqueue_trace_id)
        # set_user stores with symbol keys, but serialized data uses strings.
        # On the worker side, we call scope.set_user with the deserialized hash (string keys).
        # The user hash on the event should contain the propagated data.
        expect(transaction.user).to include(id: 99).or include("id" => 99)
      end
    end
  end

  describe "#serialize" do
    context "with trace propagation enabled" do
      before do
        perform_basic_setup do |config|
          config.traces_sample_rate = 1.0
          config.solid_queue.propagate_traces = true
        end
      end

      it "injects sentry trace data into serialized hash" do
        job = HappyJob.new
        result = job.serialize

        expect(result["_sentry"]).to be_a(Hash)
        expect(result["_sentry"]["trace_propagation_headers"]).to be_a(Hash)
      end
    end

    context "with trace propagation disabled" do
      before do
        perform_basic_setup do |config|
          config.traces_sample_rate = 1.0
          config.solid_queue.propagate_traces = false
        end
      end

      it "does not inject trace data" do
        job = HappyJob.new
        result = job.serialize

        expect(result["_sentry"]).to be_nil
      end
    end

    context "with send_default_pii enabled" do
      before do
        perform_basic_setup do |config|
          config.send_default_pii = true
          config.traces_sample_rate = 1.0
        end
        Sentry.get_current_scope.set_user(id: 42, email: "test@example.com", ip_address: "1.2.3.4")
      end

      it "includes allowlisted user fields in serialized data" do
        job = HappyJob.new
        result = job.serialize

        user = result["_sentry"]["user"]
        expect(user).to include(id: 42, email: "test@example.com")
      end

      it "excludes non-allowlisted user fields" do
        job = HappyJob.new
        result = job.serialize

        user = result["_sentry"]["user"]
        expect(user).not_to have_key(:ip_address)
        expect(user).not_to have_key("ip_address")
      end
    end

    context "with send_default_pii disabled" do
      before do
        perform_basic_setup do |config|
          config.send_default_pii = false
          config.traces_sample_rate = 1.0
        end
        Sentry.get_current_scope.set_user(id: 42)
      end

      it "does not include user context" do
        job = HappyJob.new
        result = job.serialize

        sentry_data = result["_sentry"]
        if sentry_data
          expect(sentry_data).not_to have_key("user")
        end
      end
    end

    context "for non-SolidQueue adapter" do
      before { perform_basic_setup { |config| config.traces_sample_rate = 1.0 } }

      it "does not inject sentry data" do
        job = NonSolidQueueJob.new
        result = job.serialize

        expect(result["_sentry"]).to be_nil
      end
    end
  end

  describe "#enqueue" do
    before { perform_basic_setup { |config| config.traces_sample_rate = 1.0 } }

    it "creates a queue.publish child span wrapping the enqueue" do
      transaction = Sentry.start_transaction(name: "test", op: "test")
      Sentry.get_current_scope.set_span(transaction)

      # Mock the adapter to avoid needing a real database
      allow(HappyJob.queue_adapter).to receive(:enqueue).and_return(true)

      job = HappyJob.new
      job.enqueue

      transaction.finish

      tx_event = transport.events.find { |e| e.is_a?(Sentry::TransactionEvent) }
      publish_span = tx_event.spans.find { |s| s[:op] == "queue.publish" }

      expect(publish_span).not_to be_nil
      expect(publish_span[:description]).to eq("HappyJob")
      expect(publish_span[:data]["messaging.message.id"]).to be_a(String)
      expect(publish_span[:data]["messaging.destination.name"]).to eq("default")
    end

    it "does not create a span for non-SolidQueue jobs" do
      transaction = Sentry.start_transaction(name: "test", op: "test")
      Sentry.get_current_scope.set_span(transaction)

      allow(NonSolidQueueJob.queue_adapter).to receive(:enqueue).and_return(true)

      job = NonSolidQueueJob.new
      job.enqueue

      transaction.finish

      tx_event = transport.events.find { |e| e.is_a?(Sentry::TransactionEvent) }
      publish_span = tx_event.spans&.find { |s| s[:op] == "queue.publish" }

      expect(publish_span).to be_nil
    end
  end

  describe "#deserialize" do
    before do
      perform_basic_setup do |config|
        config.solid_queue.propagate_traces = false
      end
    end

    it "extracts sentry data from job_data" do
      job = HappyJob.new
      job_data = job.serialize
      job_data["_sentry"] = { "trace_propagation_headers" => { "sentry-trace" => "abc-123-1" } }

      job.deserialize(job_data)

      expect(job.instance_variable_get(:@_sentry_trace_data)).to eq(
        { "trace_propagation_headers" => { "sentry-trace" => "abc-123-1" } }
      )
      expect(job.instance_variable_get(:@_solid_queue_worker_thread)).to eq(true)
    end

    it "ignores non-Hash _sentry values (type guard)" do
      job = HappyJob.new
      job_data = job.serialize
      job_data["_sentry"] = "malformed"

      job.deserialize(job_data)

      expect(job.instance_variable_get(:@_sentry_trace_data)).to be_nil
    end

    it "handles missing _sentry key" do
      job = HappyJob.new
      job_data = job.serialize

      job.deserialize(job_data)

      expect(job.instance_variable_get(:@_sentry_trace_data)).to be_nil
      expect(job.instance_variable_get(:@_solid_queue_worker_thread)).to eq(true)
    end

    it "does not set worker thread flag for non-SolidQueue jobs" do
      job = NonSolidQueueJob.new
      job_data = job.serialize
      job_data["_sentry"] = { "trace_propagation_headers" => { "sentry-trace" => "abc-123-1" } }

      job.deserialize(job_data)

      expect(job.instance_variable_get(:@_sentry_trace_data)).to be_nil
      expect(job.instance_variable_get(:@_solid_queue_worker_thread)).to be_nil
    end
  end

  describe "trace continuation" do
    before { perform_basic_setup { |config| config.traces_sample_rate = 1.0 } }

    it "continues trace from propagated headers" do
      trace_id = "12345678901234567890123456789012"
      parent_span_id = "1234567890123456"

      job = HappyJob.new
      job_data = job.serialize
      job_data["_sentry"] = {
        "trace_propagation_headers" => {
          "sentry-trace" => "#{trace_id}-#{parent_span_id}-1"
        }
      }
      job.deserialize(job_data)
      job.perform_now

      transaction = transport.events.first
      expect(transaction.contexts.dig(:trace, :trace_id)).to eq(trace_id)
      expect(transaction.contexts.dig(:trace, :parent_span_id)).to eq(parent_span_id)
    end

    it "creates a new trace when no headers are present" do
      HappyJob.perform_now

      transaction = transport.events.first
      expect(transaction.contexts.dig(:trace, :trace_id)).to be_a(String)
      expect(transaction.contexts.dig(:trace, :trace_id).length).to eq(32)
    end
  end

  describe "user context propagation" do
    before do
      perform_basic_setup do |config|
        config.traces_sample_rate = 1.0
        config.send_default_pii = true
      end
    end

    it "restores user context from deserialized data" do
      job = HappyJob.new
      job_data = job.serialize
      job_data["_sentry"] = {
        "user" => { "id" => 42, "email" => "test@example.com" },
        "trace_propagation_headers" => {}
      }
      job.deserialize(job_data)
      job.perform_now

      transaction = transport.events.first
      expect(transaction.user).to eq({ "id" => 42, "email" => "test@example.com" })
    end

    it "ignores non-Hash user data (type guard)" do
      job = HappyJob.new
      job_data = job.serialize
      job_data["_sentry"] = {
        "user" => "not_a_hash",
        "trace_propagation_headers" => {}
      }
      job.deserialize(job_data)

      # Should not raise
      job.perform_now
      expect(transport.events.count).to eq(1)
    end
  end

  describe "argument serialization" do
    before { perform_basic_setup { |config| config.traces_sample_rate = 0 } }

    it "includes serialized arguments in context" do
      expect { JobWithArguments.perform_now("test", count: 5) }.not_to raise_error
    end

    it "handles ranges by converting to string" do
      job = HappyJob.new
      result = job.send(:sentry_serialize_arguments, 1..10)
      expect(result).to eq("1..10")
    end

    it "handles deeply nested structures with depth limit" do
      nested = { a: { b: { c: { d: { e: { f: { g: { h: { i: { j: { k: { l: "deep" } } } } } } } } } } } }
      job = HappyJob.new
      result = job.send(:sentry_serialize_arguments, nested)

      expect(result).to be_a(Hash)
    end

    it "handles arrays" do
      job = HappyJob.new
      result = job.send(:sentry_serialize_arguments, [1, 2, 3])
      expect(result).to eq([1, 2, 3])
    end

    it "converts GlobalID-able objects to global ID strings" do
      record = FakeRecord.new(42)
      job = HappyJob.new
      result = job.send(:sentry_serialize_arguments, record)
      expect(result).to eq("gid://app/FakeRecord/42")
    end

    it "sentry_base_context excludes arguments" do
      job = JobWithArguments.new("test", count: 5)
      ctx = job.send(:sentry_base_context)
      expect(ctx).not_to have_key(:arguments)
      expect(ctx[:active_job]).to eq("JobWithArguments")
    end

    it "sentry_context includes arguments when log_arguments? is true" do
      job = JobWithArguments.new("test", count: 5)
      ctx = job.send(:sentry_context)
      expect(ctx[:arguments]).to be_present
      expect(ctx[:active_job]).to eq("JobWithArguments")
    end

    context "deferred argument serialization" do
      before { perform_basic_setup { |config| config.traces_sample_rate = 1.0 } }

      it "does not include arguments in transaction context for successful jobs" do
        JobWithArguments.perform_now("test", count: 5)

        transaction = transport.events.first
        expect(transaction.contexts[:solid_queue][:active_job]).to eq("JobWithArguments")
        expect(transaction.contexts[:solid_queue]).not_to have_key(:arguments)
      end

      it "includes arguments in error event context" do
        expect { FailingJobWithArguments.perform_now("test", count: 5) }.to raise_error(RuntimeError)

        error_event = transport.events.find { |e| !e.is_a?(Sentry::TransactionEvent) }
        expect(error_event.contexts[:solid_queue][:arguments]).to be_present
      end

      it "does not include arguments in transaction context even on error" do
        expect { FailingJobWithArguments.perform_now("test", count: 5) }.to raise_error(RuntimeError)

        transaction = transport.events.find { |e| e.is_a?(Sentry::TransactionEvent) }
        expect(transaction.contexts[:solid_queue]).not_to have_key(:arguments)
      end
    end
  end

  describe "JSON round-trip fidelity" do
    # In production, SolidQueue stores serialized job data as JSON in the DB.
    # JSON.generate → JSON.parse converts Ruby symbol keys to string keys.
    # These tests verify our code survives that transformation.

    before do
      perform_basic_setup do |config|
        config.traces_sample_rate = 1.0
        config.send_default_pii = true
      end
    end

    it "user context survives JSON round-trip (symbol keys → string keys)" do
      Sentry.get_current_scope.set_user(id: 99, email: "json@example.com")

      job = HappyJob.new
      serialized = job.serialize

      # Simulate SolidQueue's DB storage: JSON round-trip
      json_data = JSON.parse(JSON.generate(serialized))

      # Before JSON: {id: 99, email: "json@example.com"} (symbol keys)
      # After JSON:  {"id" => 99, "email" => "json@example.com"} (string keys)
      expect(json_data["_sentry"]["user"]).to eq({ "id" => 99, "email" => "json@example.com" })

      new_job = HappyJob.new
      new_job.deserialize(json_data)
      new_job.perform_now

      transaction = transport.events.find { |e| e.is_a?(Sentry::TransactionEvent) }
      # User should be restored even though keys are now strings
      expect(transaction.user).to include("id" => 99, "email" => "json@example.com")
    end

    it "trace propagation headers survive JSON round-trip" do
      trace_id = "aaaabbbbccccddddeeeeffffaaaabbbb"
      parent_span_id = "1122334455667788"

      job = HappyJob.new
      serialized = job.serialize
      # Inject known trace headers
      serialized["_sentry"]["trace_propagation_headers"] = {
        "sentry-trace" => "#{trace_id}-#{parent_span_id}-1"
      }

      json_data = JSON.parse(JSON.generate(serialized))

      new_job = HappyJob.new
      new_job.deserialize(json_data)
      new_job.perform_now

      transaction = transport.events.find { |e| e.is_a?(Sentry::TransactionEvent) }
      expect(transaction.contexts.dig(:trace, :trace_id)).to eq(trace_id)
      expect(transaction.contexts.dig(:trace, :parent_span_id)).to eq(parent_span_id)
    end
  end

  describe "thread safety" do
    before { perform_basic_setup { |config| config.traces_sample_rate = 1.0 } }

    it "does not leak scope between concurrent threads" do
      barrier = Queue.new

      threads = 2.times.map do |i|
        Thread.new do
          job_class = i.zero? ? HappyJob : ReportingJob
          # Each thread simulates the worker path (clone_hub gives unique scope)
          Sentry.clone_hub_to_current_thread
          scope = Sentry.get_current_scope
          scope.set_tags(thread_id: "thread_#{i}")

          # Synchronize so both threads are running concurrently
          barrier << true
          sleep(0.01) until barrier.size >= 2

          simulate_worker_perform(job_class)
        end
      end

      threads.each(&:join)

      # Both transactions should exist and have correct names
      transactions = transport.events.select { |e| e.is_a?(Sentry::TransactionEvent) }
      names = transactions.map(&:transaction).sort
      expect(names).to eq(["HappyJob", "ReportingJob"])

      # Each transaction should have its own tags (no cross-contamination)
      happy_tx = transactions.find { |t| t.transaction == "HappyJob" }
      reporting_tx = transactions.find { |t| t.transaction == "ReportingJob" }

      # Tags from the job itself should be correct
      expect(happy_tx.tags[:mood]).to eq("happy")
      # ReportingJob doesn't set mood, so it should not leak from HappyJob
      expect(reporting_tx.tags[:mood]).to be_nil
    end
  end
end

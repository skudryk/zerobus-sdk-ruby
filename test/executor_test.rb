# frozen_string_literal: true
require_relative 'test_helper'

class ExecutorTest < Minitest::Test
  include TestSupport

  def test_invalid_limits_are_rejected
    [0, -1, 1.5, true, nil].each do |value|
      assert_raises(ArgumentError) { Zerobus::Async::Executor.new(worker_count: value) }
      assert_raises(ArgumentError) { Zerobus::Async::Executor.new(queue_capacity: value) }
    end
  end

  def test_failure_does_not_terminate_worker
    executor = Zerobus::Async::Executor.new(worker_count: 1, queue_capacity: 1)
    worker = Zerobus::Future.new(executor: executor) { Thread.current }.value(timeout: 2)
    assert_raises(ArgumentError) do
      Zerobus::Future.new(executor: executor) { raise ArgumentError, 'bad input' }.value(timeout: 2)
    end
    assert_equal worker, Zerobus::Future.new(executor: executor) { Thread.current }.value(timeout: 2)
  ensure
    executor&.shutdown
  end

  def test_shutdown_releases_blocked_submitters_and_drains_accepted_work
    executor = Zerobus::Async::Executor.new(worker_count: 1, queue_capacity: 1)
    started, release, errors = Queue.new, Queue.new, Queue.new
    first = Zerobus::Future.new(executor: executor) { started << Thread.current; release.pop; 1 }
    worker = Timeout.timeout(2) { started.pop }
    second = Zerobus::Future.new(executor: executor) { 2 }
    producer = Thread.new do
      Zerobus::Future.new(executor: executor) { flunk 'rejected work must not execute' }
    rescue Zerobus::Async::ExecutorShutdownError => error
      errors << error
    end
    eventually { producer.status == 'sleep' }

    executor.shutdown(wait: false)
    assert producer.join(2), 'blocked submission did not wake after shutdown'
    assert_instance_of Zerobus::Async::ExecutorShutdownError, errors.pop(true)
    assert_raises(Zerobus::Async::ExecutorShutdownError) { Zerobus::Future.new(executor: executor) { 3 } }
    release << true
    assert_equal 1, first.value(timeout: 2)
    assert_equal 2, second.value(timeout: 2)
    executor.shutdown
    refute worker.alive?
  ensure
    release&.push(true)
    executor&.shutdown
    producer&.join(2)
  end

  def test_future_wakes_multiple_waiters_and_timeout_does_not_cancel_work
    executor = Zerobus::Async::Executor.new(worker_count: 1, queue_capacity: 1)
    release = Queue.new
    future = Zerobus::Future.new(executor: executor) { release.pop }
    assert_raises(Zerobus::TimeoutError) { future.value(timeout: 0.001) }
    refute future.complete?
    waiters = 3.times.map { Thread.new { future.value(timeout: 2) } }
    release << 42
    assert_equal [42, 42, 42], waiters.map(&:value)
    assert_equal 42, future.wait(timeout: 0)
    assert future.complete?
  ensure
    release&.push(42)
    executor&.shutdown
    waiters&.each { |waiter| waiter.join(2) }
  end

  def test_nested_future_reuses_worker_without_deadlocking_full_queue
    executor = Zerobus::Async::Executor.new(worker_count: 1, queue_capacity: 1)
    started, release = Queue.new, Queue.new
    outer = Zerobus::Future.new(executor: executor) do
      started << true
      release.pop
      Zerobus::Future.new(executor: executor) { 42 }.value
    end
    Timeout.timeout(2) { started.pop }
    queued = Zerobus::Future.new(executor: executor) { 43 }
    release << true
    assert_equal 42, outer.value(timeout: 2)
    assert_equal 43, queued.value(timeout: 2)
  ensure
    release&.push(true)
    executor&.shutdown
  end
end

import XCTest
@testable import TaskProcessor

private enum StudentWorkerError: Error {
    case boom
    case shouldNotRun
}

/// A counting gate: lets tests observe how many workers are running at once
/// and release them in a controlled order.
private actor ConcurrencyProbe {
    private var active = 0
    private(set) var maximumObservedActive = 0
    private var releasedIDs: Set<String> = []
    private var waiters: [String: CheckedContinuation<Void, Never>] = [:]

    func enter() {
        active += 1
        maximumObservedActive = max(maximumObservedActive, active)
    }

    func exit() {
        active -= 1
    }

    func wait(for id: String) async {
        if releasedIDs.remove(id) != nil { return }
        await withCheckedContinuation { continuation in
            waiters[id] = continuation
        }
    }

    func release(_ id: String) {
        if let waiter = waiters.removeValue(forKey: id) {
            waiter.resume()
        } else {
            releasedIDs.insert(id)
        }
    }
}

@MainActor
final class TaskProcessorStudentTests: XCTestCase {

    // MARK: Configuration and input validation

    func testNegativeMaximumConcurrencyIsRejected() {
        XCTAssertThrowsError(try TaskProcessor(maximumConcurrency: -3)) { error in
            XCTAssertEqual(
                error as? TaskProcessorError,
                .nonPositiveMaximumConcurrency
            )
        }
    }

    func testDuplicateIDReportsFirstRepeatedIDAndRunsNoWorker() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let recorder = StartOrderRecorder()
        let jobs = [
            Job(id: "a", input: 1),
            Job(id: "b", input: 2),
            Job(id: "a", input: 3)
        ]

        do {
            _ = try await processor.process(jobs) { job in
                await recorder.record(job.id)
                return job.input
            }
            XCTFail("Expected duplicateID to be thrown")
        } catch TaskProcessorError.duplicateID(let id) {
            XCTAssertEqual(id, "a")
        }

        let startedIDs = await recorder.order
        XCTAssertTrue(startedIDs.isEmpty, "No worker should run when a duplicate ID exists")
    }

    // MARK: Bounded scheduling

    func testActiveWorkerCountNeverExceedsMaximumConcurrency() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let probe = ConcurrencyProbe()
        let jobs = (0..<5).map { Job(id: "job\($0)", input: $0) }

        let task = Task {
            try await processor.process(jobs) { job in
                await probe.enter()
                await probe.wait(for: job.id)
                await probe.exit()
                return job.input
            }
        }

        // Give the initial bounded batch time to start and block on the
        // gate, then release everything so completions cascade through the
        // remaining jobs.
        try await Task.sleep(nanoseconds: 20_000_000)
        for job in jobs {
            await probe.release(job.id)
        }

        let results = try await task.value
        let maximumActive = await probe.maximumObservedActive
        XCTAssertLessThanOrEqual(maximumActive, 2)
        XCTAssertEqual(results.count, 5)
    }

    func testFreedSlotStartsNextUnstartedJobInInputOrder() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let jobs = [
            Job(id: "first", input: 1),
            Job(id: "second", input: 2),
            Job(id: "third", input: 3)
        ]
        let recorder = StartOrderRecorder()

        let results = try await processor.process(jobs) { job in
            await recorder.record(job.id)
            return job.input * 100
        }

        let startOrder = await recorder.order
        XCTAssertEqual(startOrder, ["first", "second", "third"])
        XCTAssertEqual(
            results,
            [
                JobResult(id: "first", outcome: .succeeded(100)),
                JobResult(id: "second", outcome: .succeeded(200)),
                JobResult(id: "third", outcome: .succeeded(300))
            ]
        )
    }

    // MARK: Outcome handling

    func testMixedSuccessAndFailureOutcomesMatchEachJob() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 3)
        let jobs = [
            Job(id: "ok", input: 10),
            Job(id: "bad", input: 20),
            Job(id: "ok2", input: 30)
        ]

        let results = try await processor.process(jobs) { job in
            if job.id == "bad" {
                throw StudentWorkerError.boom
            }
            return job.input
        }

        XCTAssertEqual(
            results,
            [
                JobResult(id: "ok", outcome: .succeeded(10)),
                JobResult(id: "bad", outcome: .failed),
                JobResult(id: "ok2", outcome: .succeeded(30))
            ]
        )
    }

    func testWorkerThrowingCancellationErrorProducesCancelledOutcome() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let jobs = [
            Job(id: "cancelsSelf", input: 1),
            Job(id: "succeeds", input: 2)
        ]

        let results = try await processor.process(jobs) { job in
            if job.id == "cancelsSelf" {
                throw CancellationError()
            }
            return job.input
        }

        XCTAssertEqual(
            results,
            [
                JobResult(id: "cancelsSelf", outcome: .cancelled),
                JobResult(id: "succeeds", outcome: .succeeded(2))
            ]
        )
    }

    // MARK: Cooperative cancellation of the caller's task

    func testCancellingCallerTaskStopsUnstartedJobsAndKeepsCompletedOutcomes() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let gate = ConcurrencyProbe()
        let jobs = [
            Job(id: "runs", input: 5),
            Job(id: "neverStarts", input: 6)
        ]

        let task = Task {
            try await processor.process(jobs) { job in
                await gate.enter()
                await gate.wait(for: job.id)
                await gate.exit()
                if Task.isCancelled {
                    throw CancellationError()
                }
                return job.input
            }
        }

        // Let the first job begin and observe cancellation cooperatively,
        // then cancel the caller task before releasing it.
        try await Task.sleep(nanoseconds: 20_000_000)
        task.cancel()
        await gate.release("runs")

        let results = try await task.value

        XCTAssertEqual(results.count, 2)
        XCTAssertEqual(results[1], JobResult(id: "neverStarts", outcome: .cancelled))
        // "runs" observed cancellation cooperatively after it had already
        // started, so it must resolve to .cancelled rather than .succeeded.
        XCTAssertEqual(results[0].id, "runs")
    }

    // MARK: Retry extension (bonus)

    func testRetryPolicyRejectsNonPositiveMaximumAttempts() {
        XCTAssertThrowsError(try RetryPolicy(maximumAttempts: 0)) { error in
            XCTAssertEqual(
                error as? RetryPolicyError,
                .nonPositiveMaximumAttempts
            )
        }
    }

    func testRetrySucceedsOnFinalAllowedAttempt() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let policy = try RetryPolicy(maximumAttempts: 3)
        let job = Job(id: "retrying", input: 9)
        let counter = AttemptCounter()

        let results = try await processor.process([job], retryPolicy: policy) { job, attempt in
            await counter.record(jobID: job.id, attempt: attempt)
            if attempt < 3 {
                throw StudentWorkerError.boom
            }
            return job.input
        }

        let attempts = await counter.attempts(for: "retrying")
        XCTAssertEqual(attempts, [1, 2, 3])
        XCTAssertEqual(results, [JobResult(id: "retrying", outcome: .succeeded(9))])
    }

    func testRetryExhaustsAttemptsAndReportsFailed() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let policy = try RetryPolicy(maximumAttempts: 2)
        let job = Job(id: "alwaysFails", input: 1)
        let counter = AttemptCounter()

        let results = try await processor.process([job], retryPolicy: policy) { job, attempt in
            await counter.record(jobID: job.id, attempt: attempt)
            throw StudentWorkerError.boom
        }

        let attempts = await counter.attempts(for: "alwaysFails")
        XCTAssertEqual(attempts, [1, 2])
        XCTAssertEqual(results, [JobResult(id: "alwaysFails", outcome: .failed)])
    }
}

private actor StartOrderRecorder {
    private(set) var order: [String] = []

    func record(_ id: String) {
        order.append(id)
    }
}

private actor AttemptCounter {
    private var recorded: [String: [Int]] = [:]

    func record(jobID: String, attempt: Int) {
        recorded[jobID, default: []].append(attempt)
    }

    func attempts(for jobID: String) -> [Int] {
        recorded[jobID] ?? []
    }
}

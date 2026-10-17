public struct Job: Sendable, Equatable {
    public let id: String
    public let input: Int

    public init(id: String, input: Int) {
        self.id = id
        self.input = input
    }
}

public enum JobOutcome: Sendable, Equatable {
    case succeeded(Int)
    case failed
    case cancelled
}

public struct JobResult: Sendable, Equatable {
    public let id: String
    public let outcome: JobOutcome

    public init(id: String, outcome: JobOutcome) {
        self.id = id
        self.outcome = outcome
    }
}

public enum TaskProcessorError: Error, Equatable {
    case nonPositiveMaximumConcurrency
    case duplicateID(String)
}

public struct RetryPolicy: Sendable, Equatable {
    public let maximumAttempts: Int

    public init(maximumAttempts: Int) throws {
        guard maximumAttempts > 0 else {
            throw RetryPolicyError.nonPositiveMaximumAttempts
        }
        self.maximumAttempts = maximumAttempts
    }
}

public enum RetryPolicyError: Error, Equatable {
    case nonPositiveMaximumAttempts
}

/// Carries a completed job's slot index alongside its outcome out of the
/// task group. A plain tuple can't be used here because tuple types cannot
/// conform to `Sendable`, which `withTaskGroup` requires of its result type.
private struct ScheduledOutcome: Sendable {
    let index: Int
    let outcome: JobOutcome
}

public actor TaskProcessor {
    private let maximumConcurrency: Int

    public init(maximumConcurrency: Int) throws {
        guard maximumConcurrency > 0 else {
            throw TaskProcessorError.nonPositiveMaximumConcurrency
        }
        self.maximumConcurrency = maximumConcurrency
    }

    public func process(
        _ jobs: [Job],
        worker: @escaping @Sendable (Job) async throws -> Int
    ) async throws -> [JobResult] {
        try Self.validateUniqueIDs(of: jobs)
        guard !jobs.isEmpty else { return [] }

        return await schedule(jobs) { job in
            do {
                let value = try await worker(job)
                return .succeeded(value)
            } catch is CancellationError {
                return .cancelled
            } catch {
                return .failed
            }
        }
    }

    public func process(
        _ jobs: [Job],
        retryPolicy: RetryPolicy,
        worker: @escaping @Sendable (Job, Int) async throws -> Int
    ) async throws -> [JobResult] {
        try Self.validateUniqueIDs(of: jobs)
        guard !jobs.isEmpty else { return [] }

        let maximumAttempts = retryPolicy.maximumAttempts

        return await schedule(jobs) { job in
            var attempt = 1
            while true {
                do {
                    let value = try await worker(job, attempt)
                    return .succeeded(value)
                } catch is CancellationError {
                    return .cancelled
                } catch {
                    if attempt >= maximumAttempts {
                        return .failed
                    }
                    attempt += 1
                }
            }
        }
    }

    private static func validateUniqueIDs(of jobs: [Job]) throws {
        var seenIDs = Set<String>()
        for job in jobs {
            if !seenIDs.insert(job.id).inserted {
                throw TaskProcessorError.duplicateID(job.id)
            }
        }
    }

    /// Runs `jobs` through `attempt`, never starting more than
    /// `maximumConcurrency` at once and preserving input order in the
    /// returned results. `attempt` never throws: callers translate worker
    /// errors into `JobOutcome` themselves so retry logic can live entirely
    /// in the caller-supplied closure.
    private func schedule(
        _ jobs: [Job],
        attempt: @escaping @Sendable (Job) async -> JobOutcome
    ) async -> [JobResult] {
        let outcomes = await withTaskGroup(of: ScheduledOutcome.self) { group -> [JobOutcome?] in
            var outcomes = [JobOutcome?](repeating: nil, count: jobs.count)
            var nextIndex = 0
            var cancelled = false

            func markRemainingCancelled() {
                while nextIndex < jobs.count {
                    if outcomes[nextIndex] == nil {
                        outcomes[nextIndex] = .cancelled
                    }
                    nextIndex += 1
                }
            }

            func startNext() {
                guard nextIndex < jobs.count else { return }
                let index = nextIndex
                let job = jobs[index]
                nextIndex += 1
                group.addTask {
                    let outcome = await attempt(job)
                    return ScheduledOutcome(index: index, outcome: outcome)
                }
            }

            if Task.isCancelled {
                cancelled = true
                markRemainingCancelled()
            } else {
                let limit = min(maximumConcurrency, jobs.count)
                for _ in 0..<limit {
                    startNext()
                }
            }

            while let scheduled = await group.next() {
                outcomes[scheduled.index] = scheduled.outcome

                guard !cancelled else { continue }

                if Task.isCancelled {
                    cancelled = true
                    group.cancelAll()
                    markRemainingCancelled()
                } else {
                    startNext()
                }
            }

            return outcomes
        }

        return jobs.enumerated().map { index, job in
            JobResult(id: job.id, outcome: outcomes[index] ?? .cancelled)
        }
    }
}

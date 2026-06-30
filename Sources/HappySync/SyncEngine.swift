import Foundation
import GRDB
import Supabase

/// The HappySync engine: owns the outbox drain, cursor pull, tombstones, FK ordering,
/// Realtime doorbell, status, and retry/backoff.
///
/// It does **not** own reads — the app keeps observing GRDB with `ValueObservation` — or schema.
///
/// > Scaffold (APPS-412): the public surface and the internal-table migrations are real; the
/// > sync behaviour (`start`/`enqueue`/`pullNow`) is stubbed pending APPS-413/414/415.
public actor SyncEngine {
    private let db: any DatabaseWriter
    private let supabase: SupabaseClient
    private let tables: [SyncTable]
    private let auth: @Sendable () async -> String

    /// Live engine status for the app's sync-status UI.
    public nonisolated let status: AsyncStream<SyncStatus>
    private nonisolated let statusContinuation: AsyncStream<SyncStatus>.Continuation

    private var current = SyncStatus()
    private var isRunning = false

    /// - Parameters:
    ///   - db: GRDB writer (`DatabaseQueue` or `DatabasePool`) — the local source of truth.
    ///   - supabase: Supabase client for PostgREST upsert/pull and the Realtime doorbell.
    ///   - tables: Synced tables in any order; the engine sorts them by `dependsOn`.
    ///   - auth: Returns a fresh Supabase access token, called before each authenticated batch.
    public init(
        db: any DatabaseWriter,
        supabase: SupabaseClient,
        tables: [SyncTable],
        auth: @escaping @Sendable () async -> String
    ) throws {
        self.db = db
        self.supabase = supabase
        self.tables = tables
        self.auth = auth
        (self.status, self.statusContinuation) = AsyncStream<SyncStatus>.makeStream()

        try SyncSchema.migrator().migrate(db)
        statusContinuation.yield(current)
    }

    /// Begins draining the outbox and pulling changes. Idempotent.
    public func start() {
        // ponytail: flag only for now — uploader/downloader/doorbell land in APPS-413/414/415.
        isRunning = true
    }

    /// Stops background sync and finishes the status stream.
    public func stop() {
        isRunning = false
        statusContinuation.finish()
    }

    /// Records a write in the outbox in the same transaction as the domain write.
    /// - Note: Implemented in APPS-413 (transactional write + ordered idempotent drain).
    public func enqueue(_ op: SyncOp, table: String, row: some Encodable & Sendable) throws {
        throw SyncError.notImplemented("enqueue lands in APPS-413")
    }

    /// Pulls rows changed since each table's cursor and applies them last-write-wins.
    /// - Note: Implemented in APPS-414 (tuple cursor + LWW apply + tombstones).
    public func pullNow() async throws {
        throw SyncError.notImplemented("pullNow lands in APPS-414")
    }
}

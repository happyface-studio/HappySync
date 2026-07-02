import Foundation

/// The one canonical timestamp format HappySync stores, uploads, and compares (contract §4):
/// ISO-8601, UTC, **fractional seconds** — e.g. `2026-07-02T10:00:00.123Z`.
///
/// Why a single format matters: the LWW gate compares `updatedAt` strings **lexicographically**
/// (`remote > local`), which is only correct when every value shares one format and zone. Three
/// shapes otherwise coexist — PostgREST's `…+00:00` with trimmed fractional digits, the client's
/// `…123Z`, and non-fractional legacy — and mixed comparisons are fragile. Canonicalizing every
/// stored/compared value here makes the ordering format-stable (APPS-474). It's also the encoder's
/// `Date` strategy, so a `Codable` row's `Date` never encodes as a raw `Double` (APPS-475).
///
/// Formatters are created per call: `ISO8601DateFormatter` is a non-`Sendable` class, and sync
/// timestamps are low-volume background work, so this sidesteps shared-mutable-state concerns for a
/// negligible cost. ponytail: cache behind a lock only if this ever shows up in a profile.
enum SyncTimestamp {
    private static func formatter(fractional: Bool) -> ISO8601DateFormatter {
        let f = ISO8601DateFormatter()
        f.formatOptions = fractional ? [.withInternetDateTime, .withFractionalSeconds] : [.withInternetDateTime]
        return f
    }

    /// Renders a `Date` in the canonical UTC-fractional format.
    static func string(from date: Date) -> String { formatter(fractional: true).string(from: date) }

    /// Parses an ISO-8601 timestamp — fractional or not, any offset — to a `Date`, or nil if it
    /// isn't ISO-8601 at all. The non-fractional fallback matches the contract §4 legacy read rule.
    static func date(from raw: String) -> Date? {
        formatter(fractional: true).date(from: raw) ?? formatter(fractional: false).date(from: raw)
    }

    /// Re-renders any ISO-8601 timestamp to the canonical UTC-fractional form so stored values and
    /// cursors compare lexicographically regardless of the source's format or zone. Returns the
    /// input unchanged if it can't be parsed — never drop a value we don't recognise.
    static func canonicalize(_ raw: String) -> String {
        date(from: raw).map(string(from:)) ?? raw
    }
}

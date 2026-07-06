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

    /// Whether `remote` is strictly newer than `local` for the LWW gate (contract §3/§4), compared at
    /// **microsecond** precision.
    ///
    /// `canonicalize`/`ISO8601DateFormatter`/`Date` render fractional seconds at **millisecond**
    /// resolution, but Postgres `now()` is microsecond-precise and PostgREST emits 6 fractional
    /// digits. Two writes inside the same millisecond therefore canonicalize to identical strings, so
    /// a strict `>` on the canonical form reports them equal and the newer one is skipped — and the
    /// pull's cursor advances past it regardless, so it's skipped **forever** (APPS-511, same shape as
    /// APPS-505). Parsing the fractional run by hand keeps all six digits and orders `(seconds,
    /// microseconds)` exactly.
    ///
    /// Tie-break: exactly-equal instants are **not** newer (strict). A device's own upload echo —
    /// where `stampAndClear` has already written the server's `updatedAt` to the local row — then
    /// isn't needlessly re-applied, and two genuinely distinct writes at the identical microsecond
    /// carry the same `updatedAt` and are indistinguishable to LWW anyway, so either resolution
    /// converges. See contract §4.
    ///
    /// Falls back to the canonical-string compare when either value isn't parseable ISO-8601 — never
    /// drop a value we don't recognise.
    static func isStrictlyNewer(_ remote: String, than local: String) -> Bool {
        guard let r = instant(from: remote), let l = instant(from: local) else {
            return canonicalize(remote) > canonicalize(local)
        }
        return r > l // tuple compare: whole seconds first, then the microsecond fraction
    }

    /// Parses an ISO-8601 timestamp into `(secondsSinceEpoch, microseconds)`, preserving microsecond
    /// precision that `ISO8601DateFormatter`/`Date` would truncate to milliseconds (APPS-511). The
    /// fractional run is the digits between the `.` and the zone designator (`Z`/`+`/`-`); it's
    /// right-padded/truncated to exactly six digits, so `.123` → 123000 µs and `.123456789` → 123456
    /// µs. Returns nil when the whole-second part isn't ISO-8601 we recognise (the caller then falls
    /// back to a string compare).
    private static func instant(from raw: String) -> (seconds: Int64, micros: Int64)? {
        guard let dot = raw.firstIndex(of: ".") else {
            // No fractional seconds: parse the whole-second instant directly (0 µs). `.` never appears
            // in the date/time or zone parts, so its absence means no fraction.
            guard let date = date(from: raw) else { return nil }
            return (Int64(date.timeIntervalSince1970.rounded()), 0)
        }
        let afterDot = raw.index(after: dot)
        var end = afterDot
        while end < raw.endIndex, raw[end].isNumber { end = raw.index(after: end) }
        // Rebuild a fraction-less timestamp (keeping the zone) for the whole-second parse; the run
        // between `.` and the zone is the fractional digits. Only `.` precedes the zone here, so a `-`
        // we stop at is the offset sign, never part of the date.
        guard let date = date(from: String(raw[..<dot]) + String(raw[end...])) else { return nil }
        let padded = String(raw[afterDot..<end].prefix(6)).padding(toLength: 6, withPad: "0", startingAt: 0)
        return (Int64(date.timeIntervalSince1970.rounded()), Int64(padded) ?? 0)
    }
}

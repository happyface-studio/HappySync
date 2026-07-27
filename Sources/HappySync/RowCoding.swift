import Foundation
import GRDB
import Supabase

/// Field mapping between an app's domain row and local SQLite storage. Goes through JSON so a
/// plain `[String: ...]` dictionary and a `Codable` struct encode through the same path.
enum RowCoding {
    /// Encodes an `Encodable` row into SQLite column values. Columns listed in `jsonColumns` keep
    /// their nested value as JSON text; everything else maps to a scalar.
    static func encode(_ row: some Encodable, jsonColumns: Set<String>) throws -> [String: DatabaseValue] {
        let encoder = JSONEncoder()
        // Without this, `.deferredToDate` encodes any `Date` property as a Double (seconds since
        // reference date) → stored in a TEXT timestamp column and uploaded as a JSON number, breaking
        // the §4 ISO-8601 field mapping. Encode Dates in the canonical format instead (APPS-475).
        encoder.dateEncodingStrategy = .custom { date, encoder in
            var container = encoder.singleValueContainer()
            try container.encode(SyncTimestamp.string(from: date))
        }
        let data = try encoder.encode(row)
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw SyncError.encoding("row must encode to a JSON object")
        }
        var columns: [String: DatabaseValue] = [:]
        for (key, value) in object {
            columns[key] = try sqliteValue(value, json: jsonColumns.contains(key))
        }
        return columns
    }

    private static func sqliteValue(_ value: Any, json: Bool) throws -> DatabaseValue {
        if json {
            // Keep the nested value as JSON text for a json/jsonb column. fragmentsAllowed so a
            // json column holding a bare scalar still serializes.
            let data = try JSONSerialization.data(withJSONObject: value, options: [.fragmentsAllowed])
            return String(decoding: data, as: UTF8.self).databaseValue
        }
        switch value {
        case is NSNull:
            return .null
        case let string as String:
            return string.databaseValue
        case let number as NSNumber:
            if CFGetTypeID(number) == CFBooleanGetTypeID() { return (number.boolValue ? 1 : 0).databaseValue }
            return CFNumberIsFloatType(number) ? number.doubleValue.databaseValue : number.int64Value.databaseValue
        default:
            // Dates are already ISO-8601 strings by here (the encoder's date strategy, APPS-475);
            // UUID/enum arrive as String too. Anything else (e.g. a `Data`/blob field) is
            // unsupported — see the `enqueue` shim's supported-types note.
            throw SyncError.encoding("unsupported value \(type(of: value))")
        }
    }

    /// Builds the PostgREST wire payload from a local row: the `serverOwned` columns are dropped (the
    /// table's `serverOwnedColumns` *and* its `cursorColumn` — see `SyncTable.uploadExcludedColumns`)
    /// and `jsonColumns` are parsed from text back into JSON values. When `serverColumns` is non-nil,
    /// local columns the server's schema doesn't have are dropped too, so a column the server has
    /// since removed or renamed can't reject the whole upsert (`PGRST204`) and dead-letter every write
    /// on the table (APPS-504).
    static func payload(
        from row: Row, jsonColumns: Set<String>, excluding serverOwned: Set<String>,
        restrictingTo serverColumns: Set<String>? = nil
    ) -> [String: AnyJSON] {
        var payload: [String: AnyJSON] = [:]
        for column in row.columnNames where !serverOwned.contains(column) {
            if let serverColumns, !serverColumns.contains(column) { continue }
            payload[column] = anyJSON(row[column], json: jsonColumns.contains(column))
        }
        return payload
    }

    static func anyJSON(_ value: DatabaseValue, json: Bool) -> AnyJSON {
        switch value.storage {
        case .null: return .null
        case .int64(let i): return .integer(Int(i))
        case .double(let d): return .double(d)
        case .blob: return .null // ponytail: CookThis stores no blobs; revisit if a table does.
        case .string(let s):
            if json,
               let data = s.data(using: .utf8),
               let parsed = try? JSONDecoder().decode(AnyJSON.self, from: data) {
                return parsed
            }
            return .string(s)
        }
    }

    /// Decodes a PostgREST wire row into local SQLite column values — the inverse of `payload`.
    /// Nested JSON values are stored as JSON text (matching how `encode` keeps `jsonColumns`).
    static func localColumns(from wire: [String: AnyJSON]) -> [String: DatabaseValue] {
        var columns: [String: DatabaseValue] = [:]
        for (key, value) in wire {
            columns[key] = databaseValue(value)
        }
        return columns
    }

    private static func databaseValue(_ value: AnyJSON) -> DatabaseValue {
        switch value {
        case .null: return .null
        case .bool(let b): return (b ? 1 : 0).databaseValue
        case .integer(let i): return i.databaseValue
        case .double(let d): return d.databaseValue
        case .string(let s): return s.databaseValue
        case .object, .array:
            if let data = try? JSONEncoder().encode(value) {
                return String(decoding: data, as: UTF8.self).databaseValue
            }
            return .null
        }
    }

    /// The column names of a local table. Used by the download path to intersect wire columns with
    /// the columns the local schema actually has, so a server that migrated ahead of this app build
    /// (added a column an older client lacks) doesn't brick every pull (APPS-504).
    static func tableColumns(_ db: Database, table: String) throws -> Set<String> {
        Set(try db.columns(in: table).map(\.name))
    }

    /// Upserts a row into `table` by primary key using the column values as-is. When `knownColumns`
    /// is non-nil, wire columns the local table doesn't have are silently dropped first — an older
    /// client simply doesn't see a server-added column until it updates, rather than throwing
    /// `table … has no column named …` and bricking every subsequent sync pass (APPS-504).
    static func upsertLocalRow(
        _ db: Database, table: String, primaryKey: String, columns: [String: DatabaseValue],
        restrictingTo knownColumns: Set<String>? = nil
    ) throws {
        let columns = knownColumns.map { known in columns.filter { known.contains($0.key) } } ?? columns
        guard !columns.isEmpty else { return } // nothing the local schema recognizes — skip
        let cols = Array(columns.keys)
        let quotedCols = cols.map { "\"\($0)\"" }.joined(separator: ", ")
        let placeholders = cols.map { _ in "?" }.joined(separator: ", ")
        let updates = cols.filter { $0 != primaryKey }
            .map { "\"\($0)\" = excluded.\"\($0)\"" }
            .joined(separator: ", ")
        let onConflict = updates.isEmpty ? "DO NOTHING" : "DO UPDATE SET \(updates)"
        let sql = """
            INSERT INTO "\(table)" (\(quotedCols)) VALUES (\(placeholders))
            ON CONFLICT("\(primaryKey)") \(onConflict)
            """
        try db.execute(sql: sql, arguments: StatementArguments(cols.map { columns[$0]! }))
    }
}

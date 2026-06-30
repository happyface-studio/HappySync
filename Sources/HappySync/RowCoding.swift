import Foundation
import GRDB
import Supabase

/// Field mapping between an app's domain row and local SQLite storage. Goes through JSON so a
/// plain `[String: ...]` dictionary and a `Codable` struct encode through the same path.
enum RowCoding {
    /// Encodes an `Encodable` row into SQLite column values. Columns listed in `jsonColumns` keep
    /// their nested value as JSON text; everything else maps to a scalar.
    static func encode(_ row: some Encodable, jsonColumns: Set<String>) throws -> [String: DatabaseValue] {
        let data = try JSONEncoder().encode(row)
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
            // ponytail: Date→ISO8601-fractional mapping lands with the download decode side
            // (APPS-414) so encode/decode stay symmetric; until then UUID/enum arrive as String.
            throw SyncError.encoding("unsupported value \(type(of: value))")
        }
    }

    /// Builds the PostgREST wire payload from a local row: `serverOwnedColumns` are dropped (the
    /// server owns them) and `jsonColumns` are parsed from text back into JSON values.
    static func payload(
        from row: Row, jsonColumns: Set<String>, excluding serverOwned: Set<String>
    ) -> [String: AnyJSON] {
        var payload: [String: AnyJSON] = [:]
        for column in row.columnNames where !serverOwned.contains(column) {
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

    /// Renders a primary-key `DatabaseValue` as the text stored in the outbox `pk` column.
    static func pkString(_ value: DatabaseValue) -> String {
        switch value.storage {
        case .string(let s): return s
        case .int64(let i): return String(i)
        case .double(let d): return String(d)
        case .blob, .null: return ""
        }
    }

    /// Upserts a row into `table` by primary key using the column values as-is.
    static func upsertLocalRow(
        _ db: Database, table: String, primaryKey: String, columns: [String: DatabaseValue]
    ) throws {
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

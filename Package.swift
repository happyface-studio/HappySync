// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "HappySync",
    // iOS 16 floor sits comfortably above supabase-swift's; lower it when an older app needs HappySync.
    platforms: [.iOS(.v16), .macOS(.v13)],
    products: [
        .library(name: "HappySync", targets: ["HappySync"]),
        // Fakes for the SyncRemote/SyncDoorbell seams, so a consumer can test its sync integration
        // offline. Import from a test target only — it ships no production code (issue #53).
        .library(name: "HappySyncTestSupport", targets: ["HappySyncTestSupport"]),
    ],
    dependencies: [
        .package(url: "https://github.com/groue/GRDB.swift.git", from: "7.11.0"),
        // supabase-community is the fork CookThis consumes; matching the URL (and SPM
        // identity) lets the app share one supabase-swift instead of resolving two.
        .package(url: "https://github.com/supabase-community/supabase-swift.git", from: "2.0.0"),
        // Build-tools only: `swift package generate-documentation` / `preview-documentation` for the
        // DocC catalog (issue #57). Swift Package Index builds the same catalog from `.spi.yml`.
        .package(url: "https://github.com/apple/swift-docc-plugin", from: "1.3.0"),
    ],
    targets: [
        .target(
            name: "HappySync",
            dependencies: [
                .product(name: "GRDB", package: "GRDB.swift"),
                .product(name: "Supabase", package: "supabase-swift"),
            ]
        ),
        .target(
            name: "HappySyncTestSupport",
            dependencies: [
                "HappySync",
                .product(name: "Supabase", package: "supabase-swift"),
            ]
        ),
        .testTarget(
            name: "HappySyncTests",
            dependencies: [
                "HappySync",
                // Our own tests drive sync through the same fakes consumers get, so the public
                // seams can't drift from what they're documented to do.
                "HappySyncTestSupport",
                .product(name: "GRDB", package: "GRDB.swift"),
                .product(name: "Supabase", package: "supabase-swift"),
            ]
        ),
    ]
)

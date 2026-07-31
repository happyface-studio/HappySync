// swift-tools-version: 6.0
import PackageDescription

// A runnable HappySync demo (issue #58). Deliberately a *separate* package rooted here rather than
// extra targets in the library's manifest: an app that consumes HappySync should look like one, with
// its own dependency on the published package — and a consumer opening this folder builds only the
// demo, not the library's test suite.
let package = Package(
    name: "HappySyncExamples",
    platforms: [.iOS(.v16), .macOS(.v13)],
    products: [
        // The demo's model, sync wiring, and view state — everything but the SwiftUI. Split out so
        // the sync behaviour the app shows can be asserted headlessly in CI (`DemoCoreTests`).
        .library(name: "DemoCore", targets: ["DemoCore"]),
    ],
    dependencies: [
        .package(path: ".."),
        .package(url: "https://github.com/groue/GRDB.swift.git", from: "7.11.0"),
        .package(url: "https://github.com/supabase-community/supabase-swift.git", from: "2.0.0"),
    ],
    targets: [
        .target(
            name: "DemoCore",
            dependencies: [
                .product(name: "HappySync", package: "HappySync"),
                // The demo runs against the in-memory fake, so it needs no Supabase project at all —
                // clone, `swift run`, watch two devices converge (issue #53 made this possible).
                .product(name: "HappySyncTestSupport", package: "HappySync"),
                .product(name: "GRDB", package: "GRDB.swift"),
                .product(name: "Supabase", package: "supabase-swift"),
            ]
        ),
        .executableTarget(name: "DemoApp", dependencies: ["DemoCore"]),
        // The demo doubles as an integration test: the behaviours the app shows are asserted here,
        // headlessly, against the same fake — so `swift test` in CI proves the example still works
        // rather than merely still compiling.
        .testTarget(
            name: "DemoCoreTests",
            dependencies: [
                "DemoCore",
                .product(name: "HappySync", package: "HappySync"),
                .product(name: "HappySyncTestSupport", package: "HappySync"),
                .product(name: "GRDB", package: "GRDB.swift"),
            ]
        ),
    ]
)

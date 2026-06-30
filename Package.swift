// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "HappySync",
    // iOS 16 floor sits comfortably above supabase-swift's; lower it when an older app needs HappySync.
    platforms: [.iOS(.v16), .macOS(.v13)],
    products: [
        .library(name: "HappySync", targets: ["HappySync"]),
    ],
    dependencies: [
        .package(url: "https://github.com/groue/GRDB.swift.git", from: "7.11.0"),
        .package(url: "https://github.com/supabase/supabase-swift.git", from: "2.0.0"),
    ],
    targets: [
        .target(
            name: "HappySync",
            dependencies: [
                .product(name: "GRDB", package: "GRDB.swift"),
                .product(name: "Supabase", package: "supabase-swift"),
            ]
        ),
        .testTarget(
            name: "HappySyncTests",
            dependencies: [
                "HappySync",
                .product(name: "GRDB", package: "GRDB.swift"),
                .product(name: "Supabase", package: "supabase-swift"),
            ]
        ),
    ]
)

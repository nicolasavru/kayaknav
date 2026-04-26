// Declare the plugin versions used by `app/build.gradle.kts` without
// applying them here — the actual application happens in the app
// module. This pattern (plugins declared+versioned at root, applied at
// module) is what the current Android Gradle docs recommend.
plugins {
    id("com.android.application") version "8.13.2" apply false
    // rust-android-gradle is the community-standard plugin for invoking
    // cargo-ndk from Gradle and wiring the resulting .so files into the
    // APK's jniLibs. Keeps the build single-command: `./gradlew build`
    // handles both Rust and Android.
    id("org.mozilla.rust-android-gradle.rust-android") version "0.9.6" apply false
}

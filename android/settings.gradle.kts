pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    // FAIL_ON_PROJECT_REPOS forbids module-level `repositories {}` blocks
    // so every dependency source is declared once here. Prevents silent
    // drift where one module pulls from a repo another doesn't trust.
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        google()
        mavenCentral()
    }
}

rootProject.name = "kayaknav"
include(":app")

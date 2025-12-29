/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
    `java-library`
    `maven-publish`
    `project-report`
}

val nutchVersion: String by project
val nutchName: String by project
val buildEncoding: String by project
val javaVersion: String by project

group = "org.apache.nutch"
version = nutchVersion

layout.buildDirectory.set(file("build"))

java {
    sourceCompatibility = JavaVersion.toVersion(javaVersion)
    targetCompatibility = JavaVersion.toVersion(javaVersion)
    withJavadocJar()
    withSourcesJar()
}

sourceSets {
    main {
        java {
            srcDirs("src/java")
            destinationDirectory.set(file("build/classes"))
        }
        resources {
            srcDirs("conf")
        }
    }
    test {
        java {
            srcDirs("src/test")
            destinationDirectory.set(file("build/test/classes"))
        }
        resources {
            srcDirs("src/test", "src/testresources")
        }
    }
}

// Configure test resources to match Ant classpath behavior
// In Ant, conf/ was on classpath before src/test/, so the empty conf/nutch-site.xml
// was found first. Tests relied on crawl-tests.xml and system properties for config.
tasks.processTestResources {
    // When there are duplicate files, use the one from the later source (test resources)
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
}

// =============================================================================
// Repositories
// =============================================================================
repositories {
    mavenCentral()
    maven {
        name = "ApacheSnapshots"
        url = uri("https://repository.apache.org/content/repositories/snapshots/")
    }
    maven {
        name = "Sonatype"
        url = uri("https://oss.sonatype.org/content/repositories/releases/")
    }
}

val log4jVersion = "2.25.2"
val slf4jVersion = "2.0.17"
val hadoopVersion = "3.4.2"
val cxfVersion = "3.6.9"
val jacksonVersion = "2.18.5"
val junitVersion = "5.14.1"
val junitPlatformVersion = "1.14.1"

// =============================================================================
// Use api() for dependencies that plugins need access to
// =============================================================================
dependencies {
    // Logging
    api("org.apache.logging.log4j:log4j-api:$log4jVersion")
    api("org.apache.logging.log4j:log4j-core:$log4jVersion")
    api("org.apache.logging.log4j:log4j-slf4j2-impl:$log4jVersion")
    api("org.slf4j:slf4j-api:$slf4jVersion")

    // Apache Commons
    api("org.apache.commons:commons-lang3:3.20.0")
    api("org.apache.commons:commons-collections4:4.5.0")
    api("org.apache.httpcomponents:httpclient:4.5.14")
    api("commons-httpclient:commons-httpclient:3.1")
    api("commons-codec:commons-codec:1.20.0")
    api("commons-io:commons-io:2.21.0")
    api("org.apache.commons:commons-compress:1.28.0")
    api("org.apache.commons:commons-jexl3:3.6.0")

    // T-Digest for metrics
    api("com.tdunning:t-digest:3.3")

    // Hadoop (excluding conflicting logging)
    api("org.apache.hadoop:hadoop-common:$hadoopVersion") {
        exclude(group = "ch.qos.reload4j")
        exclude(group = "org.slf4j")
    }
    api("org.apache.hadoop:hadoop-hdfs:$hadoopVersion") {
        exclude(group = "ch.qos.reload4j")
        exclude(group = "org.slf4j")
    }
    api("org.apache.hadoop:hadoop-mapreduce-client-core:$hadoopVersion") {
        exclude(group = "ch.qos.reload4j")
        exclude(group = "org.slf4j")
    }
    api("org.apache.hadoop:hadoop-mapreduce-client-jobclient:$hadoopVersion") {
        exclude(group = "ch.qos.reload4j")
        exclude(group = "org.slf4j")
    }

    // Tika
    api("org.tallison.tika:tika-core-shaded:2.9.1.0") {
        isTransitive = false
    }

    // XML
    api("xml-apis:xml-apis:1.4.01")
    api("xerces:xercesImpl:2.12.2")

    // ICU
    api("com.ibm.icu:icu4j:78.1")

    // Google
    api("com.google.guava:guava:33.5.0-jre")
    api("com.google.code.gson:gson:2.13.2")

    // Crawler Commons
    api("com.github.crawler-commons:crawler-commons:1.6")

    // WARC
    api("com.martinkl.warc:warc-hadoop:0.1.0") {
        exclude(module = "hadoop-client")
    }
    api("org.netpreserve.commons:webarchive-commons:3.0.2") {
        exclude(module = "hadoop-core")
        exclude(group = "com.google.guava")
        exclude(group = "junit")
        exclude(group = "org.json")
        exclude(group = "it.unimi.dsi", module = "dsiutils")
        exclude(group = "org.gnu.inet", module = "libidn")
    }

    // CXF (REST service)
    api("org.apache.cxf:cxf-rt-frontend-jaxws:$cxfVersion")
    api("org.apache.cxf:cxf-rt-frontend-jaxrs:$cxfVersion")
    api("org.apache.cxf:cxf-rt-transports-http:$cxfVersion")
    api("org.apache.cxf:cxf-rt-transports-http-jetty:$cxfVersion")

    // Jackson
    api("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    api("com.fasterxml.jackson.core:jackson-annotations:$jacksonVersion")
    api("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:$jacksonVersion")
    api("com.fasterxml.jackson.jaxrs:jackson-jaxrs-json-provider:$jacksonVersion")

    // HTTP components
    api("org.apache.httpcomponents:httpcore-nio:4.4.16")
    api("org.apache.httpcomponents:httpcore:4.4.16")

    // ASCII table
    api("de.vandermeer:asciitable:0.3.2")

    // Test dependencies
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testImplementation("org.junit.platform:junit-platform-launcher:$junitPlatformVersion")
    testImplementation("org.hamcrest:hamcrest:3.0")
    testImplementation("org.apache.cxf:cxf-rt-rs-client:$cxfVersion")
    testImplementation("org.eclipse.jetty:jetty-server:12.1.5") {
        exclude(group = "ch.qos.reload4j")
        exclude(module = "slf4j-reload")
    }
    testImplementation("org.littleshoot:littleproxy:1.1.2")
}

// Global exclusions
configurations.all {
    exclude(module = "jmxtools")
    exclude(module = "jms")
    exclude(module = "jmxri")
    exclude(module = "slf4j-log4j12")
    exclude(module = "log4j")
    exclude(group = "com.thoughtworks.xstream")
}

// =============================================================================
// Compilation
// =============================================================================
tasks.withType<JavaCompile>().configureEach {
    options.encoding = buildEncoding
    options.compilerArgs.add("-Xlint:-path")
}

// =============================================================================
// Copy dependencies to build/lib (matching Ant convention)
// =============================================================================
val `copy-dependencies` by tasks.registering(Copy::class) {
    description = "Copy dependencies to build/lib"
    group = "build"
    from(configurations.runtimeClasspath)
    into("build/lib")
}

// =============================================================================
// Core JAR - outputs to build/apache-nutch-{version}.jar
// =============================================================================
tasks.jar {
    archiveBaseName.set(nutchName)
    destinationDirectory.set(file("build"))
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    
    from("conf") {
        include("nutch-default.xml")
        include("nutch-site.xml")
    }
    
    dependsOn(`copy-dependencies`)
}

// =============================================================================
// Job JAR (for Hadoop) - outputs to build/apache-nutch-{version}.job
// Uses nested JARs in lib/
// =============================================================================
tasks.register<Jar>("job") {
    description = "Build nutch.job (Hadoop JAR with nested lib/)"
    group = "build"
    
    archiveBaseName.set(nutchName)
    archiveExtension.set("job")
    destinationDirectory.set(file("build"))
    
    // Depend on classes, plugins, and dependencies
    dependsOn(tasks.classes, `copy-dependencies`)
    dependsOn(subprojects.map { it.tasks.named("deploy") })
    
    // Include compiled classes (excluding config that goes at root)
    from(sourceSets.main.get().output) {
        exclude("nutch-default.xml", "nutch-site.xml")
    }
    
    // Include dependency JARs as nested JARs in lib/ (NOT unpacked - fast!)
    // Exclude Hadoop/logging JARs (provided by cluster)
    from("build/lib") {
        into("lib")
        include("*.jar")
        exclude("hadoop-*.jar", "slf4j-*.jar", "log4j-*.jar")
    }
    
    // Include plugins directory
    from("build/plugins") {
        into("classes/plugins")
    }
    
    // Include config at root (excluding templates and hadoop config)
    from("conf") {
        exclude("*.template", "hadoop*.*")
    }
    
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

// =============================================================================
// Testing
// =============================================================================
tasks.test {
    // Tests need plugins deployed first
    dependsOn("deploy-plugins")
    
    useJUnitPlatform()
    
    // Ensure consistent working directory
    workingDir = projectDir
    
    // Preserve test output directory structure
    reports.html.outputLocation.set(file("build/test-reports"))
    reports.junitXml.outputLocation.set(file("build/test-results"))
    
    // Set plugin.folders as system property with absolute path for reliable plugin discovery
    val pluginFoldersPath = file("build/plugins").absolutePath
    jvmArgs(
        "-Xmx1000m",
        "-Dplugin.folders=$pluginFoldersPath"
    )
    
    systemProperty("test.build.data", file("build/test/data").absolutePath)
    systemProperty("test.src.dir", file("src/test").absolutePath)
    systemProperty("javax.xml.parsers.DocumentBuilderFactory", 
        "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl")
    systemProperty("plugin.folders", pluginFoldersPath)
    
    // Show test output (including System.out.println from tests)
    testLogging {
        showStandardStreams = true
        events("passed", "skipped", "failed")
    }
    
    // Debug: Print plugin folder path during test execution
    doFirst {
        println("=== Nutch Test Configuration ===")
        println("Test plugin.folders path: $pluginFoldersPath")
        println("Plugin folder exists: ${file("build/plugins").exists()}")
        println("Working directory: $workingDir")
        // List deployed plugins
        val pluginsDir = file("build/plugins")
        if (pluginsDir.exists()) {
            println("Deployed plugins: ${pluginsDir.listFiles()?.map { it.name }?.sorted()}")
        }
        println("================================")
    }
}

// Copy test resources
tasks.register<Copy>("copy-test-resources") {
    description = "Copy test resources to build/test/data"
    group = "build"
    from("src/testresources")
    into("build/test/data")
}

tasks.test {
    dependsOn("copy-test-resources")
}

// =============================================================================
// Runtime directory structure
// =============================================================================
val runtime by tasks.registering {
    description = "Build runtime directories (default target)"
    group = "build"
    dependsOn(tasks.jar, "job")
    
    doLast {
        // Deploy area
        copy {
            from("build/${nutchName}-${nutchVersion}.job")
            into("runtime/deploy")
        }
        copy {
            from("src/bin")
            into("runtime/deploy/bin")
        }
        file("runtime/deploy/bin").listFiles()?.forEach { it.setExecutable(true) }
        
        // Local area
        copy {
            from("build/${nutchName}-${nutchVersion}.jar")
            into("runtime/local/lib")
        }
        copy {
            from("lib/native")
            into("runtime/local/lib/native")
        }
        copy {
            from("conf") {
                exclude("*.template")
            }
            into("runtime/local/conf")
        }
        copy {
            from("src/bin")
            into("runtime/local/bin")
        }
        file("runtime/local/bin").listFiles()?.forEach { it.setExecutable(true) }
        copy {
            from("build/lib")
            into("runtime/local/lib")
        }
        copy {
            from("build/plugins")
            into("runtime/local/plugins")
        }
        copy {
            from("build/test")
            into("runtime/local/test")
        }
    }
}

// =============================================================================
// Javadoc
// =============================================================================
tasks.javadoc {
    destinationDir = file("build/docs/api")
    
    // Depend on plugin compilation to ensure their classpaths are resolved
    dependsOn(subprojects.map { it.tasks.named("compileJava") })
    
    options {
        this as StandardJavadocDocletOptions
        windowTitle = "$nutchName $nutchVersion API"
        docTitle = "$nutchName $nutchVersion API"
        bottom = "Copyright &copy; ${project.findProperty("nutchYear")} The Apache Software Foundation"
        links("https://docs.oracle.com/en/java/javase/11/docs/api/")
        links("https://hadoop.apache.org/docs/r3.4.2/api/")
        addStringOption("-allow-script-in-comments", "-quiet")
    }
    
    // Include plugin sources
    source(fileTree("src/plugin") {
        include("*/src/java/**/*.java")
    })
    
    // Add plugin dependencies to classpath so javadoc can resolve all types
    classpath = classpath.plus(files(subprojects.flatMap { 
        it.configurations.getByName("compileClasspath").files 
    }))
}

// =============================================================================
// Distribution tasks
// =============================================================================
val `package-src` by tasks.registering(Sync::class) {
    description = "Package source distribution"
    group = "distribution"
    dependsOn(runtime, tasks.javadoc)
    
    into("dist/${nutchName}-${nutchVersion}-src")
    
    from("lib") { into("lib") }
    from("conf") { 
        into("conf") 
        exclude("*.template")
    }
    from("build/docs/api") { into("docs/api") }
    from(".") {
        include("*.txt")
    }
    from("src") { into("src") }
    from("LICENSE-binary")
    from("NOTICE-binary")
    from("licenses-binary") { into("licenses-binary") }
}

val `package-bin` by tasks.registering(Sync::class) {
    description = "Package binary distribution"
    group = "distribution"
    dependsOn(runtime, tasks.javadoc)
    
    into("dist/${nutchName}-${nutchVersion}-bin")
    
    from("runtime/local/lib") { into("lib") }
    from("runtime/local/bin") { into("bin") }
    from("runtime/local/conf") { into("conf") }
    from("build/docs/api") { into("docs/api") }
    from(".") {
        include("*.txt")
    }
    from("LICENSE-binary")
    from("NOTICE-binary")
    from("licenses-binary") { into("licenses-binary") }
    from("runtime/local/plugins") { into("plugins") }
    
    // Include plugin READMEs
    from("src/plugin") {
        include("**/README.*")
        into("plugins")
    }
}

tasks.register<Tar>("tar-src") {
    description = "Create source distribution tarball"
    group = "distribution"
    dependsOn(`package-src`)
    compression = Compression.GZIP
    archiveBaseName.set("${nutchName}-${nutchVersion}-src")
    archiveExtension.set("tar.gz")
    destinationDirectory.set(file("dist"))
    
    from("dist/${nutchName}-${nutchVersion}-src") {
        into("${nutchName}-${nutchVersion}")
        exclude("src/bin/*")
    }
    from("dist/${nutchName}-${nutchVersion}-src/src/bin") {
        into("${nutchName}-${nutchVersion}/src/bin")
        fileMode = 0b111_101_101  // 755
    }
}

tasks.register<Tar>("tar-bin") {
    description = "Create binary distribution tarball"
    group = "distribution"
    dependsOn(`package-bin`)
    compression = Compression.GZIP
    archiveBaseName.set("${nutchName}-${nutchVersion}-bin")
    archiveExtension.set("tar.gz")
    destinationDirectory.set(file("dist"))
    
    from("dist/${nutchName}-${nutchVersion}-bin") {
        into("${nutchName}-${nutchVersion}")
        exclude("bin/*")
    }
    from("dist/${nutchName}-${nutchVersion}-bin/bin") {
        into("${nutchName}-${nutchVersion}/bin")
        fileMode = 0b111_101_101  // 755
    }
}

tasks.register<Zip>("zip-src") {
    description = "Create source distribution zip"
    group = "distribution"
    dependsOn(`package-src`)
    archiveBaseName.set("${nutchName}-${nutchVersion}-src")
    destinationDirectory.set(file("dist"))
    
    from("dist/${nutchName}-${nutchVersion}-src") {
        into("${nutchName}-${nutchVersion}")
        exclude("src/bin/*")
    }
    from("dist/${nutchName}-${nutchVersion}-src/src/bin") {
        into("${nutchName}-${nutchVersion}/src/bin")
        fileMode = 0b111_101_101  // 755
    }
}

tasks.register<Zip>("zip-bin") {
    description = "Create binary distribution zip"
    group = "distribution"
    dependsOn(`package-bin`)
    archiveBaseName.set("${nutchName}-${nutchVersion}-bin")
    destinationDirectory.set(file("dist"))
    
    from("dist/${nutchName}-${nutchVersion}-bin") {
        into("${nutchName}-${nutchVersion}")
        exclude("bin/*")
    }
    from("dist/${nutchName}-${nutchVersion}-bin/bin") {
        into("${nutchName}-${nutchVersion}/bin")
        fileMode = 0b111_101_101  // 755
    }
}

// =============================================================================
// Maven publishing
// =============================================================================
publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "nutch"
            from(components["java"])
            
            pom {
                name.set("Apache Nutch")
                description.set("Nutch is an open source web-search software. It builds on Hadoop, Tika and Solr, adding web-specifics, such as a crawler, a link-graph database etc.")
                url.set("https://nutch.apache.org/")
                
                licenses {
                    license {
                        name.set("Apache 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                
                scm {
                    connection.set("scm:git:https://github.com/apache/nutch.git")
                    developerConnection.set("scm:git:https://github.com/apache/nutch.git")
                    url.set("https://github.com/apache/nutch")
                }
            }
        }
    }
    
    repositories {
        maven {
            name = "ApacheReleases"
            url = uri("https://repository.apache.org/service/local/staging/deploy/maven2")
            credentials {
                username = findProperty("apache.username") as String? ?: ""
                password = findProperty("apache.password") as String? ?: ""
            }
        }
    }
}

// =============================================================================
// Clean
// =============================================================================
tasks.register<Delete>("clean-runtime") {
    description = "Clean the runtime directory"
    group = "build"
    delete("runtime")
}

tasks.register<Delete>("clean-dist") {
    description = "Clean the dist directory"
    group = "build"
    delete("dist")
}

tasks.clean {
    dependsOn("clean-runtime", "clean-dist")
}

// =============================================================================
// Shared configuration for all plugin subprojects
// =============================================================================
subprojects {
    apply(plugin = "java-library")
    
    val subprojectName = project.name
    
    // Preserve Ant output structure: build/<plugin-name>/
    layout.buildDirectory.set(rootProject.file("build/$subprojectName"))
    
    repositories {
        mavenCentral()
        maven {
            name = "ApacheSnapshots"
            url = uri("https://repository.apache.org/content/repositories/snapshots/")
        }
    }
    
    java {
        sourceCompatibility = JavaVersion.toVersion(rootProject.findProperty("javaVersion") as String)
        targetCompatibility = JavaVersion.toVersion(rootProject.findProperty("javaVersion") as String)
    }
    
    // Disable javadoc for subprojects - root project generates combined docs
    tasks.withType<Javadoc> {
        enabled = false
    }
    
    // Source sets for plugins
    sourceSets {
        main {
            java {
                setSrcDirs(listOf("src/java"))
                destinationDirectory.set(rootProject.file("build/$subprojectName/classes"))
            }
            // Include src/java as resources to pick up properties files alongside classes
            resources {
                setSrcDirs(listOf("src/java"))
            }
        }
        test {
            java {
                setSrcDirs(listOf("src/test"))
                destinationDirectory.set(rootProject.file("build/$subprojectName/test/classes"))
            }
            // Add sample/ for config files and src/test for in-package test resources
            resources {
                setSrcDirs(listOf("sample", "src/test"))
            }
            // Output resources to same directory as classes for proper classpath resolution
            output.resourcesDir = rootProject.file("build/$subprojectName/test/classes")
        }
    }
    
    // Ensure main resource processing happens before compilation that uses those resources
    tasks.named("compileJava") {
        dependsOn(tasks.named("processResources"))
    }
    
    dependencies {
        // All plugins depend on nutch core (api dependencies are inherited)
        "implementation"(rootProject)
        
        // Test dependencies - JUnit 5 and utilities
        "testImplementation"("org.junit.jupiter:junit-jupiter-api:$junitVersion")
        "testImplementation"("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher:$junitPlatformVersion")
        "testImplementation"("org.hamcrest:hamcrest:3.0")
        
        // Root project test utilities (AbstractHttpProtocolPluginTest, etc.)
        "testImplementation"(rootProject.sourceSets.test.get().output)
    }
    
    // Plugin test compilation depends on root test compilation
    tasks.named("compileTestJava") {
        dependsOn(rootProject.tasks.named("testClasses"))
    }
    
    tasks.withType<JavaCompile>().configureEach {
        options.encoding = rootProject.findProperty("buildEncoding") as String
    }
    
    // JAR task
    tasks.jar {
        archiveBaseName.set(subprojectName)
        destinationDirectory.set(rootProject.file("build/$subprojectName"))
    }
    
    // Deploy task - copies plugin to build/plugins/<name>/
    // Only copies plugin JAR, plugin.xml, and plugin-specific dependencies
    // (NOT all inherited dependencies - those are in build/lib/)
    tasks.register<Copy>("deploy") {
        dependsOn(tasks.jar)
        
        val deployDir = rootProject.file("build/plugins/$subprojectName")
        
        into(deployDir)
        
        // Copy the plugin JAR
        from(tasks.jar)
        
        // Copy plugin.xml if it exists
        from(projectDir) {
            include("plugin.xml")
        }
        
        // Copy JARs from plugin's own lib/ directory (manual dependencies)
        from("lib") {
            include("*.jar")
        }
        
        // Copy only plugin-specific dependencies (not inherited from root)
        // by filtering out JARs that exist in root's runtimeClasspath
        val rootDeps = rootProject.configurations.runtimeClasspath.get().files.map { it.name }.toSet()
        from(configurations.runtimeClasspath) {
            exclude { rootDeps.contains(it.file.name) }
            // Also exclude the root project JAR itself
            exclude { it.file.name.startsWith("apache-nutch") }
        }
    }
    
    // Test configuration
    tasks.withType<Test>().configureEach {
        // Plugin tests need plugins deployed first and should run after core tests
        // This mimics Ant behavior where core tests ran before plugin tests
        dependsOn(rootProject.tasks.named("deploy-plugins"))
        mustRunAfter(rootProject.tasks.named("test"))
        
        useJUnitPlatform()
        
        // Run tests from root project directory so build/plugins is found
        workingDir = rootProject.projectDir
        
        jvmArgs("-Xmx1000m")
        // Point test.data to sample directory (where test files like testIndexReplace.html reside)
        systemProperty("test.data", file("sample").absolutePath)
        systemProperty("test.input", file("data").absolutePath)
        systemProperty("javax.xml.parsers.DocumentBuilderFactory",
            "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl")
        // Set plugin.folders as system property with absolute path for reliable plugin discovery
        systemProperty("plugin.folders", rootProject.file("build/plugins").absolutePath)
    }
}

// =============================================================================
// Aggregate tasks
// =============================================================================
tasks.register("deploy-plugins") {
    description = "Deploy all plugins to build/plugins/"
    group = "build"
    dependsOn(subprojects.map { it.tasks.named("deploy") })
}

tasks.register("test-plugins") {
    description = "Run JUnit tests for all plugins"
    group = "verification"
    dependsOn(subprojects.map { it.tasks.named("test") })
}

tasks.register("compile") {
    dependsOn(tasks.classes, "deploy-plugins")
    description = "Compile all Java files"
    group = "build"
}

// Compile core only
tasks.register("compile-core") {
    dependsOn(tasks.compileJava)
    description = "Compile core Java files only"
    group = "build"
}

// Compile plugins only
tasks.register("compile-plugins") {
    dependsOn("deploy-plugins")
    description = "Compile plugins only"
    group = "build"
}

// =============================================================================
// Additional testing tasks for Ant parity
// =============================================================================

// Test core only
tasks.register("test-core") {
    dependsOn(tasks.test)
    description = "Run core JUnit tests only"
    group = "verification"
}

// Test with slow tests included
tasks.register("test-full") {
    dependsOn(tasks.test, "test-plugins")
    description = "Run all JUnit tests, including slow ones"
    group = "verification"
    
    doFirst {
        tasks.test.get().systemProperty("test.include.slow", "true")
    }
}

// Test a single plugin: ./gradlew test-plugin -Pplugin=parse-html
tasks.register("test-plugin") {
    description = "Run JUnit tests for a single plugin. Usage: ./gradlew test-plugin -Pplugin=<name>"
    group = "verification"
    
    doLast {
        val pluginName = project.findProperty("plugin") as String?
            ?: throw GradleException("Plugin name required. Use: ./gradlew test-plugin -Pplugin=<name>")
        
        // Verify plugin exists
        if (subprojects.none { it.name == pluginName }) {
            throw GradleException("Plugin '$pluginName' not found")
        }
        
        // The test task was already executed via dependsOn
        println("Tests for plugin '$pluginName' completed.")
    }
}

// Make test-plugin depend on the specific plugin's test task when -Pplugin is set
gradle.taskGraph.whenReady {
    val pluginName = project.findProperty("plugin") as String?
    if (pluginName != null && hasTask(":test-plugin")) {
        val pluginProject = subprojects.find { it.name == pluginName }
        if (pluginProject != null) {
            tasks.named("test-plugin") {
                dependsOn(pluginProject.tasks.named("test"))
            }
        }
    }
}

// Nightly build
tasks.register("nightly") {
    dependsOn(tasks.test, "test-plugins", "tar-src", "zip-src")
    description = "Run the nightly target build"
    group = "build"
}

// =============================================================================
// Dependency reporting tasks
// =============================================================================

// Dependency tree
tasks.register("dependencytree") {
    dependsOn("dependencies")
    description = "Show dependency tree"
    group = "reporting"
}

// Report dependencies
tasks.register("report") {
    dependsOn("dependencies", "htmlDependencyReport")
    description = "Generate a report of dependencies"
    group = "reporting"
}

// License report
tasks.register("report-licenses") {
    description = "Generate a report of licenses of dependencies"
    group = "reporting"
    
    doLast {
        val reportFile = file("build/dependency-licenses.tsv")
        reportFile.parentFile.mkdirs()
        
        val sb = StringBuilder()
        sb.appendLine("Organisation\tModule\tRevision\tLicense")
        
        configurations.runtimeClasspath.get().resolvedConfiguration.resolvedArtifacts.forEach { artifact ->
            val id = artifact.moduleVersion.id
            sb.appendLine("${id.group}\t${id.name}\t${id.version}\t")
        }
        
        reportFile.writeText(sb.toString())
        println("License report written to: ${reportFile.absolutePath}")
    }
}

// =============================================================================
// Apache Rat license checking
// =============================================================================
val apacheRatVersion = "0.16"
val apacheRatHome = file("build/tools/apache-rat-$apacheRatVersion")

tasks.register("apache-rat-download") {
    description = "Download Apache Rat"
    group = "verification"
    
    outputs.dir(apacheRatHome)
    onlyIf { !apacheRatHome.exists() }
    
    doLast {
        val tarFile = file("build/tools/apache-rat-$apacheRatVersion-bin.tar.gz")
        val url = uri("https://archive.apache.org/dist/creadur/apache-rat-$apacheRatVersion/apache-rat-$apacheRatVersion-bin.tar.gz").toURL()
        
        file("build/tools").mkdirs()
        println("Downloading Apache Rat $apacheRatVersion...")
        url.openStream().use { input: java.io.InputStream ->
            tarFile.outputStream().use { output: java.io.OutputStream -> input.copyTo(output) }
        }
        
        copy {
            from(tarTree(tarFile))
            into("build/tools")
        }
        tarFile.delete()
    }
}

tasks.register<JavaExec>("run-rat") {
    dependsOn("apache-rat-download")
    description = "Run Apache Rat on codebase"
    group = "verification"
    
    mainClass.set("org.apache.rat.Report")
    classpath = files("$apacheRatHome/apache-rat-$apacheRatVersion.jar")
    
    args(
        "-d", "src",
        // File extensions to exclude
        "-e", ".*\\.test",
        "-e", ".*\\.txt", 
        "-e", ".*\\.properties",
        "-e", ".*\\.log",
        "-e", ".*\\.crc",
        "-e", ".*\\.urls",
        "-e", ".*\\.rules",
        "-e", ".*\\.csv",
        "-e", ".*\\.rtf",
        // Specific files to exclude
        "-e", "naivebayes-model",
        "-e", "\\.donotdelete"
    )
    
    // Output report to build directory - must be set at execution time
    doFirst {
        file("build").mkdirs()
        standardOutput = file("build/apache-rat-report.txt").outputStream()
    }
    
    doLast {
        println("Apache Rat report written to: build/apache-rat-report.txt")
    }
}

// =============================================================================
// SpotBugs static analysis
// =============================================================================
val spotbugsVersion = "4.9.8"
val spotbugsHome = file("build/tools/spotbugs-$spotbugsVersion")

tasks.register("spotbugs-download") {
    description = "Download SpotBugs"
    group = "verification"
    
    outputs.dir(spotbugsHome)
    onlyIf { !spotbugsHome.exists() }
    
    doLast {
        val tarFile = file("build/tools/spotbugs-$spotbugsVersion.tgz")
        val url = uri("https://github.com/spotbugs/spotbugs/releases/download/$spotbugsVersion/spotbugs-$spotbugsVersion.tgz").toURL()
        
        file("build/tools").mkdirs()
        println("Downloading SpotBugs $spotbugsVersion...")
        url.openStream().use { input: java.io.InputStream ->
            tarFile.outputStream().use { output: java.io.OutputStream -> input.copyTo(output) }
        }
        
        copy {
            from(tarTree(tarFile))
            into("build/tools")
        }
        tarFile.delete()
    }
}

tasks.register<JavaExec>("spotbugs") {
    dependsOn(tasks.jar, "deploy-plugins", "spotbugs-download")
    description = "Run SpotBugs source code analysis"
    group = "verification"
    
    mainClass.set("edu.umd.cs.findbugs.LaunchAppropriateUI")
    classpath = fileTree("$spotbugsHome/lib") { include("*.jar") }
    
    jvmArgs("-Xmx1024m")
    
    args(
        "-textui",
        "-html:fancy-hist.xsl",
        "-output", "build/nutch-spotbugs.html",
        "-auxclasspath", configurations.runtimeClasspath.get().asPath,
        "-sourcepath", "src/java",
        "build/${nutchName}-${nutchVersion}.jar"
    )
    
    doLast {
        println("SpotBugs report written to: build/nutch-spotbugs.html")
    }
}

// =============================================================================
// Release & Deploy (Maven Central)
// =============================================================================
tasks.register<Copy>("release") {
    dependsOn(tasks.jar, tasks.named("javadocJar"), tasks.named("sourcesJar"), tasks.javadoc)
    description = "Generate the release distribution"
    group = "publishing"
    
    into("build/release")
    
    from(tasks.jar) {
        rename { "${project.findProperty("artifactId") ?: "nutch"}-${nutchVersion}.jar" }
    }
    from(tasks.named("javadocJar")) {
        rename { "${project.findProperty("artifactId") ?: "nutch"}-${nutchVersion}-javadoc.jar" }
    }
    from(tasks.named("sourcesJar")) {
        rename { "${project.findProperty("artifactId") ?: "nutch"}-${nutchVersion}-sources.jar" }
    }
    
    doLast {
        println("Release artifacts generated in: build/release/")
    }
}

// Generate POM file
tasks.register("makepom") {
    dependsOn("generatePomFileForMavenPublication")
    description = "Generate POM file for deployment"
    group = "publishing"
    
    doLast {
        copy {
            from("build/publications/maven/pom-default.xml")
            into(".")
            rename { "pom.xml" }
        }
        println("POM file generated: pom.xml")
    }
}

// Deploy to Apache Nexus
// Note: For GPG signing, add the signing plugin and configure it
tasks.register("deploy") {
    dependsOn("release", "makepom", "publishMavenPublicationToApacheReleasesRepository")
    description = "Deploy to Apache Nexus"
    group = "publishing"
    
    doLast {
        println("Deployed to Apache Nexus. Note: GPG signing requires additional configuration.")
    }
}

// =============================================================================
// Eclipse project generation
// =============================================================================
tasks.register("eclipse") {
    description = "Create Eclipse project files"
    group = "ide"
    
    doLast {
        // Generate .project file
        val projectFile = file(".project")
        projectFile.writeText("""<?xml version="1.0" encoding="UTF-8"?>
<projectDescription>
    <name>${rootProject.name}</name>
    <comment></comment>
    <projects></projects>
    <buildSpec>
        <buildCommand>
            <name>org.eclipse.jdt.core.javabuilder</name>
        </buildCommand>
    </buildSpec>
    <natures>
        <nature>org.eclipse.jdt.core.javanature</nature>
    </natures>
</projectDescription>
""")
        
        // Generate .classpath file
        val classpathEntries = StringBuilder()
        classpathEntries.appendLine("""<?xml version="1.0" encoding="UTF-8"?>""")
        classpathEntries.appendLine("<classpath>")
        classpathEntries.appendLine("""    <classpathentry kind="src" path="src/java"/>""")
        classpathEntries.appendLine("""    <classpathentry kind="src" path="src/test" output="build/test/classes"/>""")
        classpathEntries.appendLine("""    <classpathentry kind="src" path="conf"/>""")
        
        // Add plugin sources
        file("src/plugin").listFiles()?.filter { it.isDirectory }?.forEach { plugin ->
            val srcDir = File(plugin, "src/java")
            if (srcDir.exists()) {
                classpathEntries.appendLine("""    <classpathentry kind="src" path="src/plugin/${plugin.name}/src/java"/>""")
            }
            val testDir = File(plugin, "src/test")
            if (testDir.exists()) {
                classpathEntries.appendLine("""    <classpathentry kind="src" path="src/plugin/${plugin.name}/src/test"/>""")
            }
        }
        
        // Add library JARs
        configurations.runtimeClasspath.get().files.forEach { jar ->
            classpathEntries.appendLine("""    <classpathentry kind="lib" path="${jar.absolutePath}"/>""")
        }
        
        classpathEntries.appendLine("""    <classpathentry kind="con" path="org.eclipse.jdt.launching.JRE_CONTAINER"/>""")
        classpathEntries.appendLine("""    <classpathentry kind="output" path="build/classes"/>""")
        classpathEntries.appendLine("</classpath>")
        
        file(".classpath").writeText(classpathEntries.toString())
        
        println("Eclipse project files generated: .project, .classpath")
    }
}

tasks.register<Delete>("clean-eclipse") {
    description = "Clean Eclipse project files"
    group = "ide"
    delete(".project", ".classpath", ".settings")
}

// =============================================================================
// Additional clean tasks for Ant parity
// =============================================================================
tasks.register<Delete>("clean-lib") {
    description = "Clean the project libraries directory"
    group = "build"
    delete("build/lib")
}

tasks.register<Delete>("clean-default-lib") {
    description = "Clean the project libraries directory (dependencies)"
    group = "build"
    delete("build/lib")
}

tasks.register<Delete>("clean-test-lib") {
    description = "Clean the project test libraries directory"
    group = "build"
    delete("build/test/lib")
}

tasks.register<Delete>("clean-build") {
    description = "Clean the project built files"
    group = "build"
    delete("build")
}

tasks.register<Delete>("clean-local") {
    description = "Clean the local Maven repository for this module"
    group = "build"
    
    doLast {
        val localRepo = file("${System.getProperty("user.home")}/.m2/repository/org/apache/nutch")
        if (localRepo.exists()) {
            delete(localRepo)
            println("Cleaned local Maven repository: $localRepo")
        }
    }
}

tasks.register("clean-cache") {
    description = "Delete dependency cache"
    group = "build"
    
    doLast {
        println("To clean Gradle cache, run: rm -rf ~/.gradle/caches")
    }
}

// =============================================================================
// Plugin subproject additional tasks
// =============================================================================
subprojects {
    // Dependency tree for plugin
    tasks.register("dependencytree") {
        dependsOn("dependencies")
        description = "Show dependency tree for this plugin"
        group = "reporting"
    }
    
    // Report for plugin
    tasks.register("report") {
        dependsOn("dependencies")
        description = "Generate a report of dependencies for this plugin"
        group = "reporting"
    }
    
    // License report for plugin
    tasks.register("report-licenses") {
        description = "Generate a report of licenses of dependencies for this plugin"
        group = "reporting"
        
        doLast {
            val reportFile = rootProject.file("build/${project.name}/3rd-party-licenses.tsv")
            reportFile.parentFile.mkdirs()
            
            val sb = StringBuilder()
            sb.appendLine("Organisation\tModule\tRevision")
            
            configurations.getByName("runtimeClasspath").resolvedConfiguration.resolvedArtifacts.forEach { artifact ->
                val id = artifact.moduleVersion.id
                sb.appendLine("${id.group}\t${id.name}\t${id.version}")
            }
            
            reportFile.writeText(sb.toString())
            println("License report written to: ${reportFile.absolutePath}")
        }
    }
    
    // Print plugin libraries formatted for plugin.xml
    tasks.register("print-plugin-libraries") {
        description = "Print plugin dependencies formatted for plugin.xml"
        group = "help"
        dependsOn("deploy")
        
        doLast {
            val pluginDir = rootProject.file("build/plugins/${project.name}")
            val isLibraryPlugin = project.name.startsWith("lib-")
            
            println("\n<!-- Plugin library dependencies for ${project.name} -->")
            pluginDir.listFiles()
                ?.filter { it.extension == "jar" && it.name != "${project.name}.jar" }
                ?.map { it.name }
                ?.sorted()
                ?.forEach { jarName ->
                    if (isLibraryPlugin) {
                        // Library plugins export all dependencies
                        println("      <library name=\"$jarName\">")
                        println("        <export name=\"*\"/>")
                        println("      </library>")
                    } else {
                        println("      <library name=\"$jarName\"/>")
                    }
                }
            println("<!-- End of plugin library dependencies -->")
        }
    }
}

// =============================================================================
// Nutch tasks helper - list all Nutch-specific tasks
// =============================================================================
tasks.register("nutch-tasks") {
    description = "List all Nutch-specific tasks"
    group = "help"
    
    doLast {
        val taskGroups = mapOf(
            "Build" to listOf(
                "compile", "compile-core", "compile-plugins", "jar", "job", "runtime", "nightly", "deploy-plugins"
            ),
            "Testing" to listOf(
                "test", "test-core", "test-full", "test-plugin", "test-plugins"
            ),
            "Distribution" to listOf(
                "tar-src", "tar-bin", "zip-src", "zip-bin", "package-src", "package-bin"
            ),
            "Verification" to listOf(
                "run-rat", "spotbugs", "apache-rat-download", "spotbugs-download"
            ),
            "Reporting" to listOf(
                "dependencytree", "report", "report-licenses"
            ),
            "Publishing" to listOf(
                "release", "makepom", "deploy", "javadoc", "publishToMavenLocal"
            ),
            "IDE" to listOf(
                "eclipse", "clean-eclipse"
            ),
            "Clean" to listOf(
                "clean", "clean-build", "clean-lib", "clean-default-lib", "clean-test-lib",
                "clean-local", "clean-cache", "clean-runtime", "clean-dist"
            )
        )
        
        println("\n${"=".repeat(60)}")
        println("Nutch Tasks")
        println("${"=".repeat(60)}\n")
        
        taskGroups.forEach { (groupName, taskNames) ->
            println("--- $groupName ---")
            taskNames.forEach { taskName ->
                tasks.findByName(taskName)?.let { task ->
                    val desc = task.description ?: "(no description)"
                    println("  %-22s %s".format(taskName, desc))
                }
            }
            println()
        }
        
        println("Run './gradlew <task>' to execute a task.")
        println("Run './gradlew help --task <task>' for detailed help on a task.")
        println()
        println("--- Plugin-Specific Tasks ---")
        println("  Run these on individual plugins with './gradlew :<plugin>:<task>':")
        println("  %-22s %s".format("print-plugin-libraries", "Print dependencies formatted for plugin.xml"))
        println("  %-22s %s".format("dependencytree", "Show dependency tree for plugin"))
        println("  %-22s %s".format("report-licenses", "Generate license report for plugin"))
        println()
        println("  Example: ./gradlew :indexer-solr:print-plugin-libraries")
    }
}

// Default task
defaultTasks("runtime")

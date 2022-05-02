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
    application
    base
    java
    `maven-publish`
    `java-library`
}

repositories {
    mavenCentral()
}

dependencies {
    val cxfVersion by extra { "3.4.1" }
    val hadoopVersion by extra { "3.1.3" }
    val jacksonVersion by extra { "2.12.0" }
    val log4j2Version by extra { "2.17.0" }
    val mortbayJettyVersion by extra { "6.1.26" }

    implementation("com.ibm.icu:icu4j:68.2")
    implementation("com.fasterxml.jackson.core:jackson-annotations:${jacksonVersion}")
    implementation("com.fasterxml.jackson.core:jackson-databind:${jacksonVersion}")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:${jacksonVersion}")
    implementation("com.fasterxml.jackson.jaxrs:jackson-jaxrs-json-provider:${jacksonVersion}")
    implementation("com.github.crawler-commons:crawler-commons:1.2")
    implementation("com.google.code.gson:gson:2.8.9")
    implementation("com.google.guava:guava:30.1-jre")
    implementation("com.martinkl.warc:warc-hadoop:0.1.0") {
        exclude(module="hadoop-client")
    }
    implementation("com.rabbitmq:amqp-client:5.2.0")
    implementation("com.tdunning:t-digest:3.2")
    implementation("commons-codec:commons-codec:1.15")
    implementation("de.vandermeer:asciitable:0.3.2")
    implementation("org.apache.commons:commons-collections4:4.4")
    implementation("org.apache.commons:commons-compress:1.21")
    implementation("org.apache.commons:commons-jexl3:3.1")
    implementation("org.apache.commons:commons-lang3:3.12.0")
    implementation("org.apache.cxf:cxf-rt-frontend-jaxrs:${cxfVersion}")
    implementation("org.apache.cxf:cxf-rt-frontend-jaxws:${cxfVersion}")
    implementation("org.apache.cxf:cxf-rt-transports-http:${cxfVersion}")
    implementation("org.apache.cxf:cxf-rt-transports-http-jetty:${cxfVersion}")
    implementation("org.apache.hadoop:hadoop-common:${hadoopVersion}") {
        exclude("ant", "ant")
        exclude("hsqldb", "hsqldb")
        exclude("net.java.dev.jets3t", "jets3t")
        exclude("net.sf.kosmosfs", "kfs")
        exclude("org.eclipse.jdt", "core")
        exclude("org.mortbay.jetty", "jsp-*")
    }
    implementation("org.apache.hadoop:hadoop-hdfs:${hadoopVersion}")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:${hadoopVersion}")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-jobclient:${hadoopVersion}")
    implementation("org.apache.httpcomponents:httpcore:4.4.14")
    implementation("org.apache.httpcomponents:httpcore-nio:4.4.9")
    implementation("org.apache.httpcomponents:httpclient:4.5.13")
    implementation("org.apache.logging.log4j:log4j-api:${log4j2Version}")
    implementation("org.apache.logging.log4j:log4j-core:${log4j2Version}")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:${log4j2Version}")
    implementation("org.apache.tika:tika-core:2.2.1")
    implementation("org.mortbay.jetty:jetty:${mortbayJettyVersion}")
    implementation("org.netpreserve.commons:webarchive-commons:1.1.9") {
        exclude(group="com.google.guava")
        exclude("it.unimi.dsi", "dsiutils")
            because("Incompatible LGPL 2.1 licence; exclusion disables support of WARC generation by 'bin/nutch commoncrawldump -warc ...'. Remove this exclusion and recompile Nutch to generate WARC files using the 'commoncrawldump' tool.")
        exclude(group="junit")
        exclude(module="hadoop-core")
        exclude("org.gnu.inet", "libidn")
            because("Incompatible LGPL 2.1 licence; exclusion disables support of WARC generation by 'bin/nutch commoncrawldump -warc ...'. Remove this exclusion and recompile Nutch to generate WARC files using the 'commoncrawldump' tool.")
        exclude(group="org.json")
            because("Incompatible JSON licence.")
    }
    implementation("org.slf4j:slf4j-api:1.7.35")
    implementation("xerces:xercesImpl:2.12.1")
    implementation("xml-apis:xml-apis:1.4.01") {
        because("Force this version as it is required by Tika.")
    }

    testImplementation("junit:junit:4.13.2")
    testImplementation("org.apache.cxf:cxf-rt-rs-client:${cxfVersion}")
    testImplementation("org.apache.mrunit:mrunit:1.1.0:hadoop2") {
        exclude("log4j", "log4j")
    }
    testImplementation("org.mortbay.jetty:jetty-client:${mortbayJettyVersion}")
}

application {
    // Define the main class for the application.
    //mainClass.set("nutch.App")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "org.apache.nutch"
            artifactId = "nutch"
            version = "1.18"

            from(components["java"])
        }
    }
    repositories {
        maven {
            url = uri("https://repository.apache.org/service/local/staging/deploy/maven2")
        }
    }
}

configurations {
    implementation {
        resolutionStrategy.failOnVersionConflict()
    }
}

configurations.all {
    exclude(group="com.thoughtworks.xstream")
    exclude(module="jms")
    exclude(module="jmx-tools")
    exclude(module="jmxri")
    exclude(module="log4j")
    exclude(module="slf4j-log4j12")
}

configure<JavaPluginExtension> {
    sourceCompatibility=JavaVersion.VERSION_11
    targetCompatibility=JavaVersion.VERSION_11
}

// the normal classpath
val classpathCollection: FileCollection = layout.files(
    file("${project.properties["build.classes"]}"),
    fileTree(mapOf("dir" to project.properties["build.lib.dir"], "include" to listOf("*.jar")))
)
val classPath: String = classpathCollection.asPath

// test classpath
val testClasspathCollection: FileCollection = layout.files(
    file("${project.properties["test.build.classes"]}"),
    file("${project.properties["conf.dir"]}"),
    file("${project.properties["test.src.dir"]}"),
    file("${project.properties["build.plugins"]}"),
    classpathCollection,
    file(layout.buildDirectory.dir("${project.properties["build.dir"]}/${project.properties["final.name"]}.job")),
    fileTree(mapOf("dir" to project.properties["build.lib.dir"], "include" to listOf("*.jar"))),
    fileTree(mapOf("dir" to project.properties["test.build.lib.dir"], "include" to listOf("*.jar")))
)

// legacy ant target "init" renamed to "init-nutch" to avoid gradle naming conflicts
tasks.register<Copy>("init-nutch") {
    group = "gradleBuildSystem"
    description = "Stuff required by all targets"

    // making six directories
    mkdir("${project.properties["build.dir"]}")
    mkdir("${project.properties["build.classes"]}")
    mkdir("${project.properties["build.dir"]}/release")
    mkdir("${project.properties["test.build.dir"]}")
    mkdir("${project.properties["test.build.classes"]}")
    mkdir("${project.properties["test.build.lib.dir"]}")

    // renaming from *.template to * for all files in folders in conf.dir
    from(layout.projectDirectory.dir("${project.properties["conf.dir"]}"))
    include("**/*.template")
    rename { filename: String ->
            filename.replace(".template", "")
    }
    into(layout.projectDirectory.dir("${project.properties["conf.dir"]}"))
}

tasks.register<Sync>("resolve-default") {
    group = "gradleBuildSystem"
    description = "Resolve and retrieve dependencies"
    dependsOn("clean-default-lib","init-nutch","copy-libs")
    from(configurations.compileClasspath)
    from(configurations.runtimeClasspath)
    into(layout.buildDirectory.dir("${project.properties["lib.dir"]}"))
}

tasks.register<Sync>("resolve-test") {
    group = "gradleBuildSystem"
    description = "Resolve and retrieve dependencies"
    dependsOn("clean-test-lib","init-nutch","copy-libs")
    from(configurations.testCompileClasspath)
    from(configurations.testRuntimeClasspath)
    into(layout.projectDirectory.dir("${project.properties["test.build.lib.dir"]}"))
}

tasks.register("compile") {
    group = "gradleBuildSystem"
    description = "Compile all Java files"
    dependsOn("compile-core","compile-plugins")
}

tasks.register<JavaCompile>("compile-core") {
    group = "gradleBuildSystem"
    description = "Compile core Java files only"
    dependsOn("init-nutch","resolve-default","compileJava")
    source = fileTree("${project.properties["src.dir"]}")
    include("org/apache/nutch/**/*.java")
    destinationDirectory.set(layout.projectDirectory.dir("${project.properties["build.classes"]}"))
    classpath = classpathCollection
    sourceCompatibility = "${project.properties["javac.version"]}"
    targetCompatibility = "${project.properties["javac.version"]}"
    options.annotationProcessorPath = classpathCollection
    options.sourcepath = layout.files("${project.properties["src.dir"]}")
    options.compilerArgs.add("-Xlint:-path")
    options.compilerArgs.add("-Xpkginfo:always")
    options.encoding = "${project.properties["build.encoding"]}"
    options.isDebug = "${project.properties["javac.debug"]}" == "on"
    options.isDeprecation = "${project.properties["javac.deprecation"]}" == "on"
    
    copy {
        from(layout.projectDirectory.dir("${project.properties["src.dir"]}"))
        include("**/*.html", "**/*.css", "**/*.properties")
        into(layout.projectDirectory.dir("${project.properties["build.classes"]}"))
    }
    doLast {
        delete("${project.properties["build.dir"]}/tmp")
    }
}

tasks.register<JavaExec>("proxy") {
    group = "gradleBuildSystem"
    description = "Run nutch proxy"
    dependsOn("compile-core-test","job")

    mainClass.set("org.apache.nutch.tools.proxy.ProxyTestbed")
    classpath = testClasspathCollection
    args("-fake")
    jvmArgs("-Djavax.xml.parsers.DocumentBuilderFactory=com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl")
}

tasks.register<JavaExec>("benchmark") {
    group = "gradleBuildSystem"
    description = "Run nutch benchmarking analysis"

    mainClass.set("org.apache.nutch.tools.Benchmark")
    classpath = testClasspathCollection
    jvmArgs("-Xmx512m -Djavax.xml.parsers.DocumentBuilderFactory=com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl")
    args("-maxPerHost")
    args("10")
    args("-seeds")
    args("1")
    args("-depth")
    args("5")
}

tasks.clean {
    group = "gradleBuildSystem"
    description = "Clean the project"
    dependsOn("clean-build","clean-lib","clean-dist","clean-runtime")
}

tasks.register("clean-lib") {
    group = "gradleBuildSystem"
    description = "Clean the project libraries directories (dependencies: default + test)"
    dependsOn("clean-default-lib","clean-test-lib")
}

tasks.register<Delete>("clean-default-lib") {
    group = "gradleBuildSystem"
    description = "Clean the project libraries directory (dependencies)"
    delete("${project.properties["build.lib.dir"]}")
}

tasks.register<Delete>("clean-test-lib") {
    group = "gradleBuildSystem"
    description = "Clean the project test libraries directory (dependencies)"
    delete("${project.properties["test.build.lib.dir"]}")
}

tasks.register<Delete>("clean-build") {
    group = "gradleBuildSystem"
    description = "Clean the project built files"
    delete("${project.properties["build.dir"]}")
}

tasks.register<Delete>("clean-dist") {
    group = "gradleBuildSystem"
    description = "Clean the project dist files"
    delete("${project.properties["dist.dir"]}")
}

tasks.register<Delete>("clean-runtime") {
    group = "gradleBuildSystem"
    description = "Clean the project runtime area"
    delete("${project.properties["runtime.dir"]}")
}

tasks.register<Copy>("copy-libs") {
    group = "gradleBuildSystem"
    description = "Copy the libs in lib"
    from("${project.properties["lib.dir"]}") {
        include("**/*.jar")
    }
    into("${project.properties["build.lib.dir"]}")
}

tasks.register<GradleBuild>("compile-plugins") {
    group = "gradleBuildSystem"
    description = "Compile plugins only"
    dependsOn("init-nutch","resolve-default")
    //TODO Once plugins are finished, uncomment the following lines:
    // dir = file("src/plugin")
    // tasks = listOf("deploy")
}

tasks.jar {
    group = "gradleBuildSystem"
    description = "Make nutch.jar"
    dependsOn("compile-core")

    copy {
        from(
            file(layout.projectDirectory.dir("${project.properties["conf.dir"]}/nutch-default.xml")),
            file(layout.projectDirectory.dir("${project.properties["conf.dir"]}/nutch-site.xml"))
        )
        into("${project.properties["build.classes"]}")
    }
    archiveFileName.set("${project.properties["final.name"]}.jar")
    destinationDirectory.set(layout.projectDirectory.dir("${project.properties["build.dir"]}"))
    from(files("${project.properties["build.classes"]}"))
    doLast {
        delete("${project.properties["build.dir"]}/tmp")
    }
}

tasks.register<Copy>("runtime") {
    group = "gradleBuildSystem"
    description = "Default target for running Nutch"
    dependsOn("jar","job")
    mkdir("${project.properties["runtime.dir"]}")
    mkdir("${project.properties["runtime.local"]}")
    mkdir("${project.properties["runtime.deploy"]}")

    into(layout.projectDirectory)

    into("${project.properties["runtime.deploy"]}") {
        from(layout.projectDirectory.dir("${project.properties["build.dir"]}/${project.properties["final.name"]}.job"))
    }
    into("${project.properties["runtime.deploy"]}/bin") {
        from(layout.projectDirectory.dir("src/bin"))
    }
    into("${project.properties["runtime.local"]}/lib") {
        from(layout.projectDirectory.dir("${project.properties["build.dir"]}/${project.properties["final.name"]}.jar"))
    }
    into("${project.properties["runtime.local"]}/lib/native") {
        from(layout.projectDirectory.dir("lib/native"))
    }
    into("${project.properties["runtime.local"]}/conf") {
        from(layout.projectDirectory.dir("${project.properties["conf.dir"]}")) {
            exclude("*.template")
        }
    }
    into("${project.properties["runtime.local"]}/bin") {
        from(layout.projectDirectory.dir("src/bin"))
    }
    into("${project.properties["runtime.local"]}/lib") {
        from(layout.projectDirectory.dir("${project.properties["build.lib.dir"]}"))
    }
    into("${project.properties["runtime.local"]}/plugins") {
        from(layout.projectDirectory.dir("${project.properties["build.dir"]}/plugins"))
    }
    into("${project.properties["runtime.local"]}/test") {
        from(layout.projectDirectory.dir("${project.properties["build.dir"]}/test"))
    }

    doLast() {
        project.exec() {
            commandLine("chmod","ugo+x","${project.properties["runtime.deploy"]}/bin")
            commandLine("chmod","ugo+x","${project.properties["runtime.local"]}/bin")
        }
    }
}

tasks.register<Jar>("job") {
    group = "gradleBuildSystem"
    description = "Make nutch.job jar"
    dependsOn("compile")

    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    archiveFileName.set("${project.properties["final.name"]}.job")
    destinationDirectory.set(layout.projectDirectory.dir("${project.properties["build.dir"]}"))
    from(layout.projectDirectory.dir("${project.properties["build.classes"]}")) {
        exclude("nutch-default.xml","nutch-site.xml")
    }
    from(layout.projectDirectory.dir("${project.properties["conf.dir"]}")) {
        exclude("*.template","hadoop*.*")
    }
    from(layout.projectDirectory.dir("${project.properties["build.lib.dir"]}")) {
        include("**/*.jar")
        exclude("hadoop-*.jar","slf4j*.jar","log4j*.jar")
        into("lib")
    }
    from(layout.projectDirectory.dir("${project.properties["build.plugins"]}")) {
        exclude("nutch-default.xml","nutch-site.xml")
        into("classes/plugins")
    }
    doLast {
        delete("${project.properties["build.dir"]}/tmp")
    }
}

tasks.register<JavaCompile>("compile-core-test") {
    group = "gradleBuildSystem"
    description = "Compile test code"
    dependsOn("init-nutch","compile-core","resolve-test","compileTestJava")

    source = fileTree("${project.properties["test.src.dir"]}")
    include("org/apache/nutch/**/*.java")
    destinationDirectory.set(layout.projectDirectory.dir("${project.properties["test.build.classes"]}"))
    classpath = testClasspathCollection
    sourceCompatibility = "${project.properties["javac.version"]}"
    targetCompatibility = "${project.properties["javac.version"]}"

    options.annotationProcessorPath = testClasspathCollection
    options.sourcepath = layout.files("${project.properties["src.dir"]}")
    options.compilerArgs.add("-Xlint:-path")
    options.encoding = "${project.properties["build.encoding"]}"
    options.isDebug = "${project.properties["javac.debug"]}" == "on"
    options.isDeprecation = "${project.properties["javac.deprecation"]}" == "on"
}

tasks.test.configure {
    description = "Run JUnit tests"
    dependsOn("test-core","test-plugins")
}

// Dummy target. This just ensures that the compile target always happens before the command line invocation
tasks.register<Exec>("javadocs") {
    dependsOn("compile")

    val version:String = System.getProperty("java.version")
    if("1.7.0_25".compareTo(version) >= 0)
        throw GradleException(
            "Unsupported Java version: ${version}. Javadoc requires Java version 7u25 " +
                    "or greater. See https://issues.apache.org/jira/browse/NUTCH-1590"
        )

    val org:String = "org.apache.nutch."
    val pkg:String = org+"crawl:"+
            org+"exchange:"+
            org+"fetcher:"+
            org+"hostdb:"+
            org+"indexer:"+
            org+"metadata:"+
            org+"net:"+
            org+"parse:"+
            org+"plugin:"+
            org+"protocol:"+
            org+"publisher:"+
            org+"scoring:"+
            org+"segment:"+
            org+"service:"+
            org+"tools:"+
            org+"util"

    isIgnoreExitValue = true
    commandLine(
        "javadoc",
        "-d", "build/docs/api",
        "-sourcepath", "src/java",
        "-classpath", "${project.properties["plugins.dir"]}/*;${project.properties["build.lib.dir"]}/*",
        "-author",
        "-windowtitle", "\"${project.properties["name"]} ${project.properties["version"]} API\"",
        "-doctitle", "\"${project.properties["name"]} ${project.properties["version"]} API\"",
        "-bottom", "\"Copyright &amp;copy; ${project.properties["year"]} The Apache Software Foundation\"",
        "-link", "${project.properties["javadoc.link.java"]}",
        "-link", "${project.properties["javadoc.link.hadoop"]}",
        "-group", "\"Core\"", "org.apache.nutch.*",
        "-group", "\"Plugins Api\"", "\"${project.properties["plugins.api"]}\"",
        "-group", "\"Protocol Plugins\"", "\"${project.properties["plugins.protocol"]}\"",
        "-group", "\"URL Filter Plugins\"", "\"${project.properties["plugins.urlfilter"]}\"",
        "-group", "\"URL Normalizer Plugins\"", "\"${project.properties["plugins.urlnormalizer"]}\"",
        "-group", "\"Scoring Plugins\"", "\"${project.properties["plugins.scoring"]}\"",
        "-group", "\"Parse Plugins\"", "\"${project.properties["plugins.parse"]}\"",
        "-group", "\"Parse Filter Plugins\"", "\"${project.properties["plugins.parsefilter"]}\"",
        "-group", "\"Publisher Plugins\"", "\"${project.properties["plugins.publisher"]}\"",
        "-group", "\"Exchange Plugins\"", "\"${project.properties["plugins.exchange"]}\"",
        "-group", "\"Indexing Filter Plugins\"", "\"${project.properties["plugins.index"]}\"",
        "-group", "\"Indexer Plugins\"", "\"${project.properties["plugins.indexer"]}\"",
        "-group", "\"Misc. Plugins\"", "\"${project.properties["plugins.misc"]}\"",
        "-overview", "\"${project.properties["src.dir"]}/overview.html\"",
        "-use",
        "--allow-script-in-comments",
        "-J-DproxyPort=",
        "-J-DproxyHost=",
        "-subpackages",pkg
    )
}
tasks.javadoc.configure {
    group = "gradleBuildSystem"
    description = "Generate Javadoc"
    dependsOn("javadocs")
}

tasks.register<Copy>("package-src")
{
    group = "gradleBuildSystem"
    description = "Generate source distribution package"
    dependsOn("runtime","javadoc")

    destinationDir = file(".")

    mkdir("${project.properties["dist.dir"]}")
    mkdir("${project.properties["src.dist.version.dir"]}")
    mkdir("${project.properties["src.dist.version.dir"]}/lib")
    mkdir("${project.properties["src.dist.version.dir"]}/docs")
    mkdir("${project.properties["src.dist.version.dir"]}/docs/api")
    mkdir("${project.properties["src.dist.version.dir"]}/ivy")

    from("lib") {
        includeEmptyDirs = false
        into("${project.properties["src.dist.version.dir"]}/lib")
    }
    from("${project.properties["conf.dir"]}") {
        exclude("**/*.template")
        into("${project.properties["src.dist.version.dir"]}/conf")
    }
    from("${project.properties["build.javadoc"]}") {
        into("${project.properties["src.dist.version.dir"]}/docs/api")
    }
    from(".") {
        include("*.txt")
        into("${project.properties["src.dist.version.dir"]}")
    }
    from("src") {
        includeEmptyDirs = true
        into("${project.properties["src.dist.version.dir"]}/src")
    }
    from(".") {
        include("build.gradle.kts","gradle.properties","settings.gradle.kts")
        into("${project.properties["src.dist.version.dir"]}/")
    }
}
tasks.register<Zip>("zip-src")
{
    group = "gradleBuildSystem"
    description = "Generate src.zip distribution package"
    dependsOn("package-src")

    archiveFileName.set("${project.properties["src.dist.version.dir"]}.zip")
    destinationDirectory.set(layout.buildDirectory.dir("${project.properties["final.name"]}"))

    from(
        files("${project.properties["src.dist.version.dir"]}") {
            fileMode = 664
            exclude("src/bin/*")
            //TODO delete the following line once Ivy is removed completely
            exclude("ivy")
            include("**")
        },
        files("${project.properties["src.dist.version.dir"]}") {
            fileMode = 755
            include("src/bin/*")
        }
    )
}

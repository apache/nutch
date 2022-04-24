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
    description = "Resolve and retrieve dependencies"
    dependsOn("clean-default-lib","init-nutch","copy-libs")
    from(configurations.compileClasspath)
    from(configurations.runtimeClasspath)
    into(layout.buildDirectory.dir("${project.properties["lib.dir"]}"))
}

tasks.register<Sync>("resolve-test") {
    description = "Resolve and retrieve dependencies"
    dependsOn("clean-test-lib","init-nutch","copy-libs")
    from(configurations.testCompileClasspath)
    from(configurations.testRuntimeClasspath)
    into(layout.projectDirectory.dir("${project.properties["test.build.lib.dir"]}"))
}

tasks.register("compile") {
    description = "Compile all Java files"
    dependsOn("compile-core","compile-plugins")
}

tasks.register<JavaCompile>("compile-core") {
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
    description = "Run nutch proxy"
    dependsOn("compile-core-test","job")

    mainClass.set("org.apache.nutch.tools.proxy.ProxyTestbed")
    classpath = testClasspathCollection
    args("-fake")
    jvmArgs("-Djavax.xml.parsers.DocumentBuilderFactory=com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl")
}

tasks.register<JavaExec>("benchmark") {
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
    description = "Clean the project"
    dependsOn("clean-build","clean-lib","clean-dist","clean-runtime")
}

tasks.register("clean-lib") {
    description = "Clean the project libraries directories (dependencies: default + test)"
    dependsOn("clean-default-lib","clean-test-lib")
}

tasks.register<Delete>("clean-default-lib") {
    description = "Clean the project libraries directory (dependencies)"
    delete("${project.properties["build.lib.dir"]}")
}

tasks.register<Delete>("clean-test-lib") {
    description = "Clean the project test libraries directory (dependencies)"
    delete("${project.properties["test.build.lib.dir"]}")
}

tasks.register<Delete>("clean-build") {
    description = "Clean the project built files"
    delete("${project.properties["build.dir"]}")
}

tasks.register<Delete>("clean-dist") {
    description = "Clean the project dist files"
    delete("${project.properties["dist.dir"]}")
}

tasks.register<Delete>("clean-runtime") {
    description = "Clean the project runtime area"
    delete("${project.properties["runtime.dir"]}")
}

tasks.register<Copy>("copy-libs") {
    description = "Copy the libs in lib"
    from("${project.properties["lib.dir"]}") {
        include("**/*.jar")
    }
    into("${project.properties["build.lib.dir"]}")
}

tasks.register<GradleBuild>("compile-plugins") {
    description = "Compile plugins only"
    dependsOn("init-nutch","resolve-default")
    //TODO Once plugins are finished, uncomment the following lines:
    // dir = file("src/plugin")
    // tasks = listOf("deploy")
}

tasks.jar {
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

    doLast {
        project.exec {
            commandLine("chmod","ugo+x","${project.properties["runtime.deploy"]}/bin")
            commandLine("chmod","ugo+x","${project.properties["runtime.local"]}/bin")
        }
    }
}

tasks.register<Jar>("job") {
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

tasks.test.configure() {
    description = "Run JUnit tests"
    dependsOn("test-core","test-plugins")
}

tasks.javadoc {
    //TODO This function is untested because the equivalent ant target does not work
    description = "generate Javadoc"
    dependsOn("compile")

    val version:String = System.getProperty("java.version")
    if("1.7.0_25".compareTo(version) >= 0)
        throw GradleException(
            "Unsupported Java version: ${version}. Javadoc requires Java version 7u25 " +
            "or greater. See https://issues.apache.org/jira/browse/NUTCH-1590"
        )

    mkdir("${project.properties["build.javadoc"]}")
    mkdir("${project.properties["build.javadoc"]}/resources")

    options {
        overview = "${project.properties["src.dir"]}/overview.html"
        destinationDirectory = file("${project.properties["build.javadoc"]}")
        windowTitle = "${project.properties["name"]} ${project.properties["version"]} API"
        isFailOnError = true
        //TODO isFailOnWarning = true

        val dc = StandardJavadocDocletOptions()
        dc.isAuthor = true
        dc.isVersion = true
        dc.isUse = true
        dc.docTitle = windowTitle
        dc.bottom = "Copyright &amp;copy; ${project.properties["year"]} The Apache Software Foundation"
        dc.addStringOption("${project.properties["javadoc.proxy.host"]}")
        dc.addStringOption("${project.properties["javadoc.proxy.port"]}")
        dc.addBooleanOption("--allow-script-in-comments")
        dc.links(
            "${project.properties["javadoc.link.java"]}",
            "${project.properties["javadoc.link.hadoop"]}"
        )
        dc.classpath = files("${project.properties["build.plugins"]}") {
            include("**/*.jar")
            exclude("any23/javax.annotation-api*.jar")
        }.distinct()
        dc.groups = mutableMapOf(
            "Core"                    to mutableListOf("org.apache.nutch.*"),
            "Plugins API"             to mutableListOf("${project.properties["plugins.api"]}"),
            "Protocol Plugins"        to mutableListOf("${project.properties["plugins.protocol"]}"),
            "URL Filter Plugins"      to mutableListOf("${project.properties["plugins.urlfilter"]}"),
            "URL Normalizer Plugins"  to mutableListOf("${project.properties["plugins.urlnormalizer"]}"),
            "Scoring Plugins"         to mutableListOf("${project.properties["plugins.scoring"]}"),
            "Parse Plugins"           to mutableListOf("${project.properties["plugins.parse"]}"),
            "Parse Filter Plugins"    to mutableListOf("${project.properties["plugins.parsefilter"]}"),
            "Publisher Plugins"       to mutableListOf("${project.properties["plugins.publisher"]}"),
            "Exchange Plugins"        to mutableListOf("${project.properties["plugins.exchange"]}"),
            "Indexing Filter Plugins" to mutableListOf("${project.properties["plugins.index"]}"),
            "Indexer Plugins"         to mutableListOf("${project.properties["plugins.indexer"]}"),
            "Misc. Plugins"           to mutableListOf("${project.properties["plugins.misc"]}")
        )
        // TODO it is unclear if this actually gets applied to the javadoc options
        doclet = dc.doclet
    }

    //TODO this is meant to replace the <packageset> tags
    include(
        "${project.properties["src.dir"]}",
        "${project.properties["plugins.dir"]}/any23/src/java/",
        "${project.properties["plugins.dir"]}/creativecommons/src/java",
        "${project.properties["plugins.dir"]}/feed/src/java",
        "${project.properties["plugins.dir"]}/headings/src/java",
        "${project.properties["plugins.dir"]}/exchange-jexl/src/java",
        "${project.properties["plugins.dir"]}/index-anchor/src/java",
        "${project.properties["plugins.dir"]}/index-basic/src/java",
        "${project.properties["plugins.dir"]}/index-geoip/src/java",
        "${project.properties["plugins.dir"]}/index-jexl-filter/src/java",
        "${project.properties["plugins.dir"]}/index-links/src/java",
        "${project.properties["plugins.dir"]}/index-metadata/src/java",
        "${project.properties["plugins.dir"]}/index-more/src/java",
        "${project.properties["plugins.dir"]}/index-replace/src/java",
        "${project.properties["plugins.dir"]}/index-static/src/java",
        "${project.properties["plugins.dir"]}/indexer-cloudsearch/src/java/",
        "${project.properties["plugins.dir"]}/indexer-csv/src/java",
        "${project.properties["plugins.dir"]}/indexer-dummy/src/java",
        "${project.properties["plugins.dir"]}/indexer-elastic/src/java/",
        "${project.properties["plugins.dir"]}/indexer-kafka/src/java/",
        "${project.properties["plugins.dir"]}/indexer-rabbit/src/java",
        "${project.properties["plugins.dir"]}/indexer-solr/src/java",
        "${project.properties["plugins.dir"]}/language-identifier/src/java",
        "${project.properties["plugins.dir"]}/lib-htmlunit/src/java",
        "${project.properties["plugins.dir"]}/lib-http/src/java",
        "${project.properties["plugins.dir"]}/lib-rabbitmq/src/java",
        "${project.properties["plugins.dir"]}/lib-regex-filter/src/java",
        "${project.properties["plugins.dir"]}/lib-selenium/src/java",
        "${project.properties["plugins.dir"]}/microformats-reltag/src/java",
        "${project.properties["plugins.dir"]}/mimetype-filter/src/java",
        "${project.properties["plugins.dir"]}/parse-ext/src/java",
        "${project.properties["plugins.dir"]}/parse-html/src/java",
        "${project.properties["plugins.dir"]}/parse-js/src/java",
        "${project.properties["plugins.dir"]}/parse-metatags/src/java",
        "${project.properties["plugins.dir"]}/parse-swf/src/java",
        "${project.properties["plugins.dir"]}/parse-tika/src/java",
        "${project.properties["plugins.dir"]}/parse-zip/src/java",
        "${project.properties["plugins.dir"]}/parsefilter-debug/src/java",
        "${project.properties["plugins.dir"]}/parsefilter-naivebayes/src/java",
        "${project.properties["plugins.dir"]}/parsefilter-regex/src/java",
        "${project.properties["plugins.dir"]}/protocol-file/src/java",
        "${project.properties["plugins.dir"]}/protocol-ftp/src/java",
        "${project.properties["plugins.dir"]}/protocol-htmlunit/src/java",
        "${project.properties["plugins.dir"]}/protocol-http/src/java",
        "${project.properties["plugins.dir"]}/protocol-httpclient/src/java",
        "${project.properties["plugins.dir"]}/protocol-interactiveselenium/src/java",
        "${project.properties["plugins.dir"]}/protocol-okhttp/src/java",
        "${project.properties["plugins.dir"]}/protocol-selenium/src/java",
        "${project.properties["plugins.dir"]}/publish-rabbitmq/src/java",
        "${project.properties["plugins.dir"]}/scoring-depth/src/java",
        "${project.properties["plugins.dir"]}/scoring-link/src/java",
        "${project.properties["plugins.dir"]}/scoring-opic/src/java",
        "${project.properties["plugins.dir"]}/scoring-orphan/src/java",
        "${project.properties["plugins.dir"]}/scoring-similarity/src/java",
        "${project.properties["plugins.dir"]}/scoring-metadata/src/java",
        "${project.properties["plugins.dir"]}/subcollection/src/java",
        "${project.properties["plugins.dir"]}/tld/src/java",
        "${project.properties["plugins.dir"]}/urlfilter-automaton/src/java",
        "${project.properties["plugins.dir"]}/urlfilter-domain/src/java",
        "${project.properties["plugins.dir"]}/urlfilter-domaindenylist/src/java",
        "${project.properties["plugins.dir"]}/urlfilter-fast/src/java",
        "${project.properties["plugins.dir"]}/urlfilter-ignoreexempt/src/java",
        "${project.properties["plugins.dir"]}/urlfilter-prefix/src/java",
        "${project.properties["plugins.dir"]}/urlfilter-regex/src/java",
        "${project.properties["plugins.dir"]}/urlfilter-suffix/src/java",
        "${project.properties["plugins.dir"]}/urlfilter-validator/src/java",
        "${project.properties["plugins.dir"]}/urlmeta/src/java",
        "${project.properties["plugins.dir"]}/urlnormalizer-ajax/src/java",
        "${project.properties["plugins.dir"]}/urlnormalizer-basic/src/java",
        "${project.properties["plugins.dir"]}/urlnormalizer-host/src/java",
        "${project.properties["plugins.dir"]}/urlnormalizer-pass/src/java",
        "${project.properties["plugins.dir"]}/urlnormalizer-protocol/src/java",
        "${project.properties["plugins.dir"]}/urlnormalizer-querystring/src/java",
        "${project.properties["plugins.dir"]}/urlnormalizer-regex/src/java",
        "${project.properties["plugins.dir"]}/urlnormalizer-slash/src/java"
    )

    copy {
        from("${project.properties["plugins.dir"]}/plugin.dtd")
        into("${project.properties["build.javadoc"]}/org/apache/nutch/plugin/doc-files")
    }
    copy {
        from(
            "${project.properties["conf.dir"]}/nutch-default.xml",
            "${project.properties["conf.dir"]}/configuration.xsl"
        )
        into("${project.properties["build.javadoc"]}/resources/")
    }
}

tasks.register<Copy>("package-src")
{
    //TODO This function is untested because of the dependency on the javadoc target
    description = "generate source distribution package"
    dependsOn("runtime","javadoc")

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
    //TODO skipped Ivy copy, now deprecated
    from(".") {
        //TODO replaced Ant files with Gradle files
        include("build.gradle.kts","gradle.properties","settings.gradle.kts")
        into("${project.properties["src.dist.version.dir"]}/")
    }
}
tasks.register<Zip>("zip-src")
{
    description = "generate src.zip distribution package"
    dependsOn("package-src")

    archiveFileName.set("${project.properties["src.dist.version.dir"]}.zip")
    destinationDirectory.set(layout.buildDirectory.dir("${project.properties["final.name"]}"))

    from(
        files("${project.properties["src.dist.version.dir"]}") {
            fileMode = 664
            exclude("src/bin/*")
            exclude("ivy/ivy*.jar")
            include("**")
        },
        files("${project.properties["src.dist.version.dir"]}") {
            fileMode = 755
            include("src/bin/*")
        }
    )
}
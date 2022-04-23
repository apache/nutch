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
    `maven-publish`
    application
    java
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
    fileTree("${project.properties["conf.dir"]}").matching { include("**/*.template") }.forEach { file: File ->
        rename { fileName: String ->
            fileName.replace(".template", "")
        }
    }
}

tasks.register<Sync>("resolve-default") {
    description = "Resolve and retrieve dependencies"
    dependsOn("clean-default-lib","init-nutch","copy-libs")
    from(configurations.compileClasspath)
    from(configurations.runtimeClasspath)
    into(layout.buildDirectory.dir("${project.properties["lib.dir"]}"))
}

tasks.register("compile") {
    description = "Compile all Java files"
    dependsOn("compile-core","compile-plugins")
}

tasks.register<JavaCompile>("compile-core") {
    description = "Compile core Java files only"
    dependsOn("init-nutch","resolve-default")

    source = fileTree("${project.properties["src.dir"]}")
    include("org/apache/nutch/**/*.java")
    // destinationDirectory = DirectoryProperty("${project.properties["build.classes"]}")
    destinationDirectory.set(layout.projectDirectory.dir("${project.properties["build.classes"]}"))
    classpath = classpathCollection
    sourceCompatibility = "${project.properties["javac.version"]}"
    targetCompatibility = "${project.properties["javac.version"]}"
    
    options.annotationProcessorPath = classpathCollection
    options.sourcepath = layout.files("${project.properties["src.dir"]}")
    options.compilerArgs.add("-Xlint:-path")
    options.encoding = "${project.properties["build.encoding"]}"
    options.isDebug = "${project.properties["javac.debug"]}" == "on"
    options.isDeprecation = "${project.properties["javac.deprecation"]}" == "on"

    copy {
        from("${project.properties["src.dir"]}")
        include("**/*.html")
        include("**/*.css")
        include("**/*.properties")
        into("${project.properties["build.classes"]}")
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

// tasks.javadoc {
//     description = "Generate Javadoc"
//     dependsOn("compile")
//     mkdir("${project.properties["build.javadoc"]}")
//     mkdir("${project.properties["build.javadoc"]}/resources")
// }

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

    from("${project.properties["conf.dir"]}/nutch-default.xml")
    into("${project.properties["build.classes"]}")

    from("${project.properties["conf.dir"]}/nutch-site.xml")
    into("${project.properties["build.classes"]}")

    //TODO this is meant to replace <jar jarfile="${build.dir}/${final.name}.jar" basedir="${build.classes}">
    destinationDirectory.set(file("${project.properties["build.dir"]}/${project.properties["final.name"]}.jar"))
    from(project.properties["build.classes"] as String)
}

tasks.register<Copy>("runtime")
{
    description = "Default target for running Nutch"
    dependsOn("jar","job")
    mkdir("${project.properties["runtime.dir"]}")
    mkdir("${project.properties["runtime.local"]}")
    mkdir("${project.properties["runtime.deploy"]}")

    from(layout.buildDirectory.dir("${project.properties["build.dir"]}/${project.properties["final.name"]}.job"))
    into(layout.buildDirectory.dir("${project.properties["runtime.deploy"]}"))

    from(layout.buildDirectory.dir("${project.properties["runtime.deploy"]}/bin"))
    into(layout.buildDirectory.dir("src/bin"))

    from(layout.buildDirectory.dir("${project.properties["build.dir"]}/${project.properties["final.name"]}.jar"))
    into(layout.buildDirectory.dir("${project.properties["runtime.local"]}/lib"))

    from(layout.buildDirectory.dir("${project.properties["runtime.local"]}/lib/native"))
    into(layout.buildDirectory.dir("lib/native"))

    from(layout.buildDirectory.dir("${project.properties["runtime.local"]}/conf")) {
        exclude("*.template")
    }
    into(layout.buildDirectory.dir("${project.properties["conf.dir"]}/lib"))

    from(layout.buildDirectory.dir("${project.properties["runtime.local"]}/bin"))
    into(layout.buildDirectory.dir("src/bin"))

    from(layout.buildDirectory.dir("${project.properties["runtime.local"]}/lib"))
    into(layout.buildDirectory.dir("${project.properties["build.dir"]}/lib"))

    from(layout.buildDirectory.dir("${project.properties["runtime.local"]}/plugins"))
    into(layout.buildDirectory.dir("${project.properties["build.dir"]}/plugins"))

    from(layout.buildDirectory.dir("${project.properties["runtime.local"]}/test"))
    into(layout.buildDirectory.dir("${project.properties["build.dir"]}/test"))

    doLast() {
        project.exec() {
            commandLine("chmod","ugo+x","${project.properties["runtime.deploy"]}/bin")
            commandLine("chmod","ugo+x","${project.properties["runtime.local"]}/bin")
        }
    }
}

tasks.register<Jar>("job")
{
    //TODO there is no support to create a ".job" directly
    description = "Make nutch.job jar"
    dependsOn("compile")

    from(
        files("${project.properties["build.classes"]}") {
            exclude("nutch-default.xml","nutch-site.xml")
        },
        files("${project.properties["conf.dir"]}") {
            exclude("*.template","hadoop*.*")
        },
        files("${project.properties["build.lib.dir"]}") {
            eachFile {
                relativePath = RelativePath(true,"lib")
            }
            include("**/*.jar")
            exclude("hadoop-*.jar,slf4j*.jar","log4j*.jar")
        },
        files("${project.properties["build.plugins"]}") {
            eachFile {
                relativePath = RelativePath(true,"classes","plugins")
            }
        }
    )
    into("${project.properties["build.dir"]}/${project.properties["final.name"]}.job")
}

tasks.register<JavaCompile>("compile-core-test")
{
    description = "Compile test code"
    dependsOn("init-nutch","compile-core","resolve-test")
    source = fileTree(layout.buildDirectory.dir("${project.properties["test.src.dir"]}"))
    include("org/apache/nutch/**/*.java")
    destinationDirectory.set(layout.projectDirectory.dir("${project.properties["build.classes"]}"))
    classpath = classpathCollection
    sourceCompatibility = "${project.properties["javac.version"]}"
    targetCompatibility = "${project.properties["javac.version"]}"

    options.annotationProcessorPath = classpathCollection
    options.sourcepath = layout.files("${project.properties["src.dir"]}")
    options.compilerArgs.add("-Xlint:-path")
    options.isDebug = "${project.properties["javac.debug"]}" == "on"
    options.encoding = "${project.properties["build.encoding"]}"
    options.isDeprecation = "${project.properties["javac.deprecation"]}" == "on"
}

tasks.test.configure()
{
    description = "Run JUnit tests"
    dependsOn("test-core","test-plugins")
}
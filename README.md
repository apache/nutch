Apache Nutch README
===================

[![master pull request ci](https://github.com/apache/nutch/actions/workflows/master-build.yml/badge.svg)](https://github.com/apache/nutch/actions/workflows/master-build.yml)

<img src="https://nutch.apache.org/assets/img/nutch_logo_tm.png" align="right" width="300" />

For the latest information about Nutch, please visit our website at:

   https://nutch.apache.org/

and our wiki, at:

   https://cwiki.apache.org/confluence/display/NUTCH/Home

To get started using Nutch read Tutorial:

   https://cwiki.apache.org/confluence/display/NUTCH/NutchTutorial

Building Nutch
==============

Nutch uses [Gradle](https://gradle.org/) for build and dependency management. The Gradle wrapper is included, so you don't need to install Gradle separately.

### Prerequisites

- Java 11 or higher

### Available Tasks

To see all Nutch-specific tasks organized by category:

```bash
./gradlew nutch-tasks
```

This displays tasks for building, testing, distribution, verification, reporting, publishing, IDE setup, and cleaning.

To see all Gradle tasks (including standard Gradle tasks):

```bash
./gradlew tasks --all
```

To get detailed help on a specific task:

```bash
./gradlew help --task <taskName>
```

Common tasks:

| Task | Description |
|------|-------------|
| `runtime` | Build runtime directories (default) |
| `jar` | Build nutch.jar |
| `job` | Build nutch.job (Hadoop fat JAR) |
| `test` | Run core tests |
| `test-plugins` | Run all plugin tests |
| `javadoc` | Generate Javadoc |
| `clean` | Clean all build artifacts |

### Creating Distributions

```bash
# Source distribution
./gradlew tar-src
./gradlew zip-src

# Binary distribution
./gradlew tar-bin
./gradlew zip-bin
```

Distributions are created in the `dist/` directory.

Upgrading Dependencies
======================

Plugin dependencies are managed in `gradle.properties` and plugin-specific `build.gradle.kts` files. When upgrading a dependency, you must also update the plugin's `plugin.xml` to list the resolved JAR files.

### General Upgrade Process

1. **Update the version** in `gradle.properties`:
   ```properties
   solrVersion=9.0.0
   ```

2. **Generate the library entries** for `plugin.xml`:
   ```bash
   ./gradlew :indexer-solr:print-plugin-libraries
   ```

3. **Update `plugin.xml`** — copy the output between the appropriate marker comments (e.g., `<!-- Solr dependencies -->` and `<!-- end of Solr dependencies -->`)

4. **Build and test**:
   ```bash
   ./gradlew clean test :indexer-solr:test
   ```

### Checking for Dependency Conflicts

After upgrading, check for version conflicts:

```bash
# Full dependency tree
./gradlew dependencies

# Check specific plugin
./gradlew :indexer-solr:dependencies

# Generate HTML report
./gradlew report
```

Review `build/reports/project/dependencies/root.html` for a visual dependency tree.

Contributing
============
To contribute a patch, follow these instructions (note that installing
[Hub](https://hub.github.com/) is not strictly required, but is recommended).

0. Download and install hub.github.com
1. File JIRA issue for your fix at https://issues.apache.org/jira/projects/NUTCH/issues
   - you will get issue id NUTCH-xxxx where xxxx is the issue ID.
2. `git clone https://github.com/apache/nutch.git`
3. `cd nutch`
4. `git checkout -b NUTCH-xxxx`
5. edit files (please try and include a test case if possible)
6. `git status` (make sure it shows what files you expected to edit)
7. Make sure that your code complies with the [Nutch codeformatting template](https://raw.githubusercontent.com/apache/nutch/master/eclipse-codeformat.xml), which is basially two space indents
8. `git add <files>`
9. `git commit -m "fix for NUTCH-xxx contributed by <your username>"`
10. `hub fork` (if hub is not installed, you can fork the project using the "fork" button on the [Nutch Github project page](https://github.com/apache/nutch))
11. `git push -u <your git username> NUTCH-xxxx`
12. `hub pull-request` (if hub is not installed, please follow the instructions how to [create a pull-request from a fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork))


IDE setup
=========

### Eclipse

Import Nutch as a Gradle project:

1. Select **File > Import > Gradle > Existing Gradle Project**
2. Select the nutch directory and click **Finish**
3. Eclipse will automatically import all modules

You must [configure the nutch-site.xml](https://cwiki.apache.org/confluence/display/NUTCH/RunNutchInEclipse) before running. Make sure you've added `http.agent.name` and `plugin.folders` properties. The `plugin.folders` normally points to `<project_root>/build/plugins`.

Now create a Java Application Configuration, choose `org.apache.nutch.crawl.Injector`, add two paths as arguments. First one is the crawldb directory, second one is the URL directory where the injector can read urls. Now run your configuration.

If you see `No plugins found on paths of property plugin.folders="plugins"`, update the `plugin.folders` configuration in nutch-default.xml.


### IntelliJ IDEA

Import Nutch as a Gradle project:

1. Select **File > Open** and select the nutch directory
2. IntelliJ will detect the Gradle build and import the project automatically
3. Wait for the Gradle sync to complete

Alternatively, select **File > New > Project from Existing Sources**, select the nutch directory, and choose **Import project from external model > Gradle**.

**N.B.** For anyone on a Mac with a homebrew-installed openjdk, you need to use the directory under _libexec_: `<openjdk11_directory>/libexec/openjdk.jdk/Contents/Home`.

To import the code-style, go to **IntelliJ IDEA > Preferences > Editor > Code Style > Java**.

For the Scheme dropdown select "Project". Click the gear icon and select **Import Scheme > Eclipse XML file**.

Select the `eclipse-codeformat.xml` file and click "Open". On next screen check the "Current Scheme" checkbox and hit OK.

### Running in IntelliJ IDEA

- Open **Run/Debug Configurations**
- Select "+" to create a new configuration and select "Application"
- For "Main Class" enter a class with a main function (e.g. `org.apache.nutch.indexer.IndexingJob`)
- For "Program Arguments" add the arguments needed for the class. You can get these by running the crawl executable for your job. Use fully-qualified paths. (e.g. `/Users/user/nutch/crawl/crawldb /Users/user/nutch/crawl/segments/20221222160141 -deleteGone`)
- For "Working Directory" enter your nutch `runtime/local` directory
- Select **Modify options > Modify Classpath** and add the config directory belonging to the "Working Directory" from the previous step (e.g. `/Users/user/nutch/runtime/local/conf`). This will allow the resource loader to load that configuration.
- Select **Modify options > Add VM Options**. Add the VM options needed. You can get these by running the crawl executable for your job (e.g. `-Xmx4096m -Dhadoop.log.dir=/Users/user/nutch/runtime/local/logs -Dhadoop.log.file=hadoop.log -Dmapreduce.job.reduces=2 -Dmapreduce.reduce.speculative=false -Dmapreduce.map.speculative=false -Dmapreduce.map.output.compress=true`)

**Note**: IntelliJ automatically compiles code when you run. To ensure plugins are deployed, run `./gradlew deploy-plugins` before running Nutch commands that require plugins.

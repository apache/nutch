# Agent guidance

This file is read by automated agents (security scanners, code
analyzers, AI assistants) operating on this repository. It
points them at the human-authored references they should
consult before producing output.

## Security

Security model: [SECURITY.md](./SECURITY.md), which links to
the canonical model document at
<https://nutch.apache.org/documentation/security/#security-model>.

Agents that scan this repository should consult the linked
security model for the project's threat model, in-scope /
out-of-scope declarations, and known non-findings before
reporting issues.

## Repository Layout

- `src/java/`: Nutch core classes.
- `src/bin/`: shell scripts to run Nutch.
- `src/test/`: core unit tests.
- `src/testresources/`: resources for core unit tests.
- `src/plugin/`: Nutch plugins with subfolders for the implementation,
  unit tests and test resources.

## Requirements

- Nutch requires Java 17.

## Building and Testing

Apache [Ant](https://ant.apache.org/) is required to compile and test Nutch.

1. Build

   ```bash
   ant clean runtime
   ```

2. Test

   ```bash
   ant clean test
   ```

   Note: the target `clean` is not required, but recommended to ensure
   that the tests are run in a clean environment.
   - Full tests (runs long):

     ```bash
     ant clean test-full
     ```

   - Integration tests (protocol and indexer plugins):

     ```bash
     ant clean test-protocol-integration
     ant clean test-indexer-integration
     ```

   - Run tests of a single unit test class (here `TestCrawlDbDeduplication`):

     ```bash
     ant test-core -Dtestcase=TestCrawlDbDeduplication
     ```

   - Run tests of a single plugin (here `protocol-okhttp`):

     ```bash
     ant test-plugin -Dplugin=protocol-okhttp
     ```

## Java Code Formatting

Java source code should follow the
[Nutch Eclipse Code Formatting rules](./eclipse-codeformat.xml).

## Commit Messages and Pull Requests

- Please follow the instructions in the
  [Nutch Pull Request Template](./.github/pull_request_template.md).
- Every [Jira issue](https://issues.apache.org/jira/projects/NUTCH/summary)
  and accompanying pull request should answer:
  - If it's about a bug:
    - A detailed description and
    - the necessary steps and environment to reproduce the bug.
  - If it's about a new feature or improvement:
    - Which core component or plugin is changed?
    - Why is the proposed change is necessary?
    - Are there any breaking changes introduced?

More tips how to get involved into the Nutch web crawler project are
shared in the Nutch wiki at [Becoming A Nutch
Developer](https://cwiki.apache.org/confluence/display/NUTCH/Becoming+A+Nutch+Developer).

## About Nutch Plugins

Plugins allow anyone to extend the functionality of Nutch by writing
their own implementation of a given interface, whether it's a protocol
to download content, a parser to parse the content, or and indexer to
forward the content to a search index. But there are more plugin
interfaces or "extension points".

Each plugin has its own Java class loader, which separates the plugin
from other plugins. Only the Nutch core classes are visible to all
plugins. However, a plugin can request the "import" of the classes and
dependencies of another plugin. Shared plugins are typically called
`lib-xyz`, for example, `lib-http` which provides shared
functionalities for all HTTP protocol plugins. The separation of the
Java class path and the dependencies of a plugin allows to run a
specific setup of Nutch with a cleaner and lean class path and memory
footprint.

The technical details of the Nutch plugin system are shared in the
Nutch wiki at [Plugin
Central](https://cwiki.apache.org/confluence/display/NUTCH/PluginCentral)
and subpages of it.

## Running Nutch

To run Nutch, you need to install the Nutch binary package or compile
the Nutch sources.  For convenience, point the environment variable
`NUTCH_HOME` to the Nutch runtime. That's the root folder of the
unpacked binary package or the folder `runtime/local/` if Nutch is
built from the Java sources. There's also `runtime/deploy/` in case
you want to run Nutch on a Hadoop cluster.

Once `NUTCH_HOME` is defined you can call

```bash
$NUTCH_HOME/bin/nutch
```

It will show you a list of available Nutch tools. Then you can call
the tool without additional arguments to get a list of available
command-line options:

```bash
$NUTCH_HOME/bin/nutch parsechecker
```

Then you can put the arguments together, for example, to fetch and
parse the Nutch homepage:

```bash
$NUTCH_HOME/bin/nutch parsechecker \
    -followRedirects -checkRobotsTxt -dumpText https://nutch.apache.org/
```

For more information on this short example, see the Nutch wiki page
[Quick Start
Parsechecker](https://cwiki.apache.org/confluence/display/NUTCH/QuickStartparseChecker).

Detailed instructions to run Nutch to crawl content and index it into
Solr are shared in the [Nutch
Tutorial](https://cwiki.apache.org/confluence/display/NUTCH/NutchTutorial).

## Nutch Configuration

There are two primary Nutch configuration files:

- `conf/nutch-default.xml`: This file contains the default values and
  descriptions of the Nutch configuration properties. A tabular view
  of this file, rendered per XSLT, is found
  [here](https://nutch.apache.org/documentation/javadoc/api/resources/nutch-default.xml).
- `conf/nutch-site.xml`: This file contains site-specific overrides of
  Nutch configuration properties. Only properties different from the
  default settings should be configured in this file.

The configuration files are loaded from the Java classpath. The first
occurrence of a file on the classpath is used.

In addition, all Nutch tools (implementing
[org.apache.hadoop.util.Tool](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/util/Tool.html))
allow to override configuration properties per job by passing
`-Dproperty=value` in front of all other command-line
options. Multiple configuration options can be set by passing `-D...`
multiple times.

More information about Nutch configuration is found on the Nutch wiki at

- <https://cwiki.apache.org/confluence/display/NUTCH/Home#Home-Configuration>
- <https://cwiki.apache.org/confluence/display/NUTCH/NutchConfigurationFiles>


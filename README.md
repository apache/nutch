Apache Nutch README
===================

[![master pull request ci][ci-badge]][ci-link]
[![Quality Gate Status][sonar-badge]][sonar-link]

[ci-badge]: https://github.com/apache/nutch/actions/workflows/master-build.yml/badge.svg
[ci-link]: https://github.com/apache/nutch/actions/workflows/master-build.yml
[sonar-badge]: https://sonarcloud.io/api/project_badges/measure?project=apache_nutch&metric=alert_status
[sonar-link]: https://sonarcloud.io/summary/new_code?id=apache_nutch

![Nutch logo][logo]

[logo]: https://nutch.apache.org/assets/img/nutch_logo_tm.png

For the latest information about Nutch, please visit our website at:

   <https://nutch.apache.org/>

and our wiki, at:

   <https://cwiki.apache.org/confluence/display/NUTCH/Home>

To get started using Nutch read Tutorial:

   <https://cwiki.apache.org/confluence/display/NUTCH/NutchTutorial>

Contributing
------------

To contribute a patch, follow these instructions (note that installing
[Hub](https://hub.github.com/) is not strictly required, but is recommended).

0. Download and install hub.github.com
1. File JIRA issue for your fix at
   <https://issues.apache.org/jira/projects/NUTCH/issues>
   - you will get issue id NUTCH-xxxx where xxxx is the issue ID.
2. `git clone https://github.com/apache/nutch.git`
3. `cd nutch`
4. `git checkout -b NUTCH-xxxx`
5. edit files (please try and include a test case if possible)
6. `git status` (make sure it shows what files you expected to edit)
7. Make sure that your code complies with the [Nutch codeformatting
   template][eclipse-format], which is basically two space indents
8. `git add <files>`
9. `git commit -m "fix for NUTCH-xxx contributed by <your username>"`
10. `hub fork` (if hub is not installed, fork using the "fork" button on the
    [Nutch Github project page](https://github.com/apache/nutch))
11. `git push -u <your git username> NUTCH-xxxx`
12. `hub pull-request` (if hub is not installed, please follow the
    instructions to [create a pull-request from a fork][pr-from-fork])

Pre-commit / Apache Yetus
-------------------------

Pull requests run [Apache Yetus](https://yetus.apache.org/) test-patch for
automated checks (style, reporting). See
[Basic Precommit](https://yetus.apache.org/documentation/0.15.1/precommit/)
and [Usage Introduction][yetus-usage]. CI uses Java 17. To run test-patch
locally (e.g. before opening a PR):

```bash
test-patch --basedir=/path/to/clean/repo --build-tool=nobuild \
  --plugins=all,-jira,-gitlab,-unit,-compile [patchfile]
```

Exclude patterns can be added in `.yetus/excludes.txt` (regex, one per line).

IDE setup
---------

### Eclipse

Generate Eclipse project files

```bash
ant eclipse
```

and follow the instructions in [Importing existing projects][eclipse-import].

You must [configure the nutch-site.xml][runnutch] before running. Make sure you
have added `http.agent.name` and `plugin.folders` properties. The
plugin.folders normally points to `<project_root>/build/plugins`.

Now create a Java Application Configuration, choose
org.apache.nutch.crawl.Injector, add two paths as arguments: first the crawldb
directory, second the URL directory where the injector can read urls. Then run
your configuration.

If we still see "No plugins found on paths of property plugin.folders=plugins",
update the plugin.folders in the nutch-default.xml; this is a quick fix, but
should not be used.

### Intellij IDEA

First install the [IvyIDEA Plugin][ivyidea]. Then run `ant eclipse`. This
creates the .classpath and .project files so Intellij can import the project.

In Intellij IDEA, select File > New > Project from Existing Sources. Select the
nutch home directory and click "Open".

On the "Import Project" screen select the "Import project from external model"
radio button and select "Eclipse". Click "Create". On the next screen the
"Eclipse projects directory" should be already set to the nutch folder. Leave
the "Create module files near .classpath files" radio button selected.

Click "Next" on the next screens. On the project SDK screen select Java 11 and
click "Create". **N.B.** On Mac with homebrew openjdk, use the directory under
_libexec_: `<openjdk11_directory>/libexec/openjdk.jdk/Contents/Home`.

Once the project is imported, you will see a popup saying "Ant build scripts
found", "Frameworks detected - IvyIDEA Framework detected". Click "Import". If
you don't get the pop-up, go through the steps again as this happens from time
to time. There is another Ant popup that asks you to configure the project. Do
NOT click "Configure".

To import the code-style: Intellij IDEA > Preferences > Editor > Code Style >
Java. For the Scheme dropdown select "Project". Click the gear icon and select
"Import Scheme" > "Eclipse XML file". Select the eclipse-format.xml file and
click "Open". On the next screen check the "Current Scheme" checkbox and hit OK.

### Running in Intellij IDEA

Running in Intellij

- Open Run/Debug Configurations
- Select "+" to create a new configuration and select "Application"
- For "Main Class" enter a class with a main function
  (e.g. org.apache.nutch.indexer.IndexingJob)
- For "Program Arguments" add the arguments needed for the class. You can get
  these by running the crawl executable for your job. Use full-qualified paths
  (e.g. crawldb and segments paths plus -deleteGone)
- For "Working Directory" enter your nutch runtime/local path
- Select "Modify options" > "Modify Classpath" and add the config directory for
  that Working Directory (e.g. runtime/local/conf)
- Select "Modify options" > "Add VM Options" and add the VM options from
  running the crawl executable (e.g. -Xmx4096m -Dhadoop.log.dir=... etc.)

**Note**: You will need to manually trigger a build through ANT to get latest
updated changes when running, because the ant build system is separate from
the Intellij one.

[eclipse-import]: https://help.eclipse.org/2019-06/topic/org.eclipse.platform.doc.user/tasks/tasks-importproject.htm
[eclipse-format]: https://raw.githubusercontent.com/apache/nutch/master/eclipse-codeformat.xml
[pr-from-fork]: https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork
[runnutch]: https://cwiki.apache.org/confluence/display/NUTCH/RunNutchInEclipse
[ivyidea]: https://plugins.jetbrains.com/plugin/3612-ivyidea
[yetus-usage]: https://yetus.apache.org/documentation/0.15.1/precommit/usage-intro/

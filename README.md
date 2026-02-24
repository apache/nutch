Apache Nutch README
===================

[![master pull request ci](https://github.com/apache/nutch/actions/workflows/master-build.yml/badge.svg)](https://github.com/apache/nutch/actions/workflows/master-build.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=apache_nutch&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=apache_nutch)

<img src="https://nutch.apache.org/assets/img/nutch_logo_tm.png" align="right" width="300" />

For the latest information about Nutch, please visit our website at:

   https://nutch.apache.org/

and our wiki, at:

   https://cwiki.apache.org/confluence/display/NUTCH/Home

To get started using Nutch read Tutorial:

   https://cwiki.apache.org/confluence/display/NUTCH/NutchTutorial

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

Generate Eclipse project files

```
ant eclipse
```

and follow the instructions in [Importing existing projects](https://help.eclipse.org/2019-06/topic/org.eclipse.platform.doc.user/tasks/tasks-importproject.htm).

You must [configure the nutch-site.xml](https://cwiki.apache.org/confluence/display/NUTCH/RunNutchInEclipse) before running. Make sure, you've added ```http.agent.name``` and ```plugin.folders``` properties. The plugin.folders normally points to ```<project_root>/build/plugins```.

Now create a Java Application Configuration, choose org.apache.nutch.crawl.Injector, add two paths as arguments. First one is the crawldb directory, second one is the URL directory where, the injector can read urls. Now run your configuration.

If we still see the ```No plugins found on paths of property plugin.folders="plugins"```, update the plugin.folders in the nutch-default.xml, this is a quick fix, but should not be used.


### Intellij IDEA

First install the [IvyIDEA Plugin](https://plugins.jetbrains.com/plugin/3612-ivyidea). then run ```ant eclipse```. This will create the necessary
.classpath and .project files so that Intellij can import the project in the next step.

In Intellij IDEA, select File > New > Project from Existing Sources. Select the nutch home directory and click "Open".

On the "Import Project" screen select the "Import project from external model" radio button and select "Eclipse".
Click "Create". On the next screen the "Eclipse projects directory" should be already set to the nutch folder.
Leave the "Create module files near .classpath files" radio button selected.
Click "Next" on the next screens. On the project SDK screen select Java 11 and click "Create".
**N.B.** For anyone on a Mac with a homebrew-installed openjdk, you need to use the directory under _libexec_: `<openjdk11_directory>/libexec/openjdk.jdk/Contents/Home`.

Once the project is imported, you will see a popup saying "Ant build scripts found", "Frameworks detected - IvyIDEA Framework detected". Click "Import".
If you don't get the pop-up, I'd suggest going through the steps again as this happens from time to time. There is another
Ant popup that asks you to configure the project. Do NOT click "Configure".

To import the code-style, Go to Intellij IDEA > Preferences > Editor > Code Style > Java.

For the Scheme dropdown select "Project". Click the gear icon and select "Import Scheme" > "Eclipse XML file".

Select the eclipse-format.xml file and click "Open". On next screen check the "Current Scheme" checkbox and hit OK.

### Running in Intellij IDEA

Running in Intellij

- Open Run/Debug Configurations
- Select "+" to create a new configuration and select "Application"
- For "Main Class" enter a class with a main function (e.g. org.apache.nutch.indexer.IndexingJob).
- For "Program Arguments" add the arguments needed for the class. You can get these by running the crawl executable for your job. Use full-qualified paths. (e.g. /Users/kamil/workspace/external/nutch/crawl/crawldb /Users/kamil/workspace/external/nutch/crawl/segments/20221222160141 -deleteGone)
- For "Working Directory" enter "/Users/kamil/workspace/external/nutch/runtime/local".
- Select "Modify options" > "Modify Classpath" and add the config directory belonging to the "Working Directory" from the previous step (e.g. /Users/kamil/workspace/external/nutch/runtime/local/conf). This will allow the resource loader to load that configuration.
- Select "Modify options" > "Add VM Options". Add the VM options needed. You can get these by running the crawl executable for your job (e.g. -Xmx4096m -Dhadoop.log.dir=/Users/kamil/workspace/external/nutch/runtime/local/logs -Dhadoop.log.file=hadoop.log -Dmapreduce.job.reduces=2 -Dmapreduce.reduce.speculative=false -Dmapreduce.map.speculative=false -Dmapreduce.map.output.compress=true)

**Note**: You will need to manually trigger a build through ANT to get latest updated changes when running. This is because the ant build system is separate from the Intellij one.

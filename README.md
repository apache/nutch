# Apache Nutch README

<img src="https://nutch.apache.org/assets/img/nutch_logo_tm.png" align="right" width="300" />

For the latest information about Nutch, please visit our website at:

   https://nutch.apache.org/

and our wiki, at:

   https://cwiki.apache.org/confluence/display/NUTCH/Home

To get started using Nutch read Tutorial:

   https://cwiki.apache.org/confluence/display/NUTCH/NutchTutorial

# Running Locally

Go to the directory.
```bash
cd $WORKSPACE_HOME/external/nutch
```

Build the project (this will create the runtime) folder.

```bash
ant clean runtime
```

Create a directory for the crawl
```bash
mkdir crawl
```

Create seed file
```bash
echo "https://nutch.apache.org/" > crawl/seed.txt
```

(Optional) Edit crawl file to reduce steps. I often remove the linking and deduping steps near the end.
```bash
mate runtime/local/bin/crawl
```

Ensure you have minimum settings in nutch-site.xml.
```bash
mate runtime/local/conf/nutch-site.xml
```

Add the http.agent.name key to nutch-site.xml.
```xml
<property>
  <name>http.agent.name</name>
  <value>etesting</value>
  <description>
  </description>
</property>
```

(Optional) You might want to adjust output locations for the index writers in.

```bash
mate runtime/local/conf/index-writers.xml
```


Run the job (1 iteration)
```bash
runtime/local/bin/crawl -i -s crawl/seed.txt -D plugin.includes='indexer-json|index-basic|protocol-http|parse-html' crawl 1
```

# Contributing

To contribute a patch, follow these instructions (note that installing
[Hub](https://hub.github.com/) is not strictly required, but is recommended).

```
0. Download and install hub.github.com
1. File JIRA issue for your fix at https://issues.apache.org/jira/projects/NUTCH/issues
- you will get issue id NUTCH-xxx where xxx is the issue ID.
2. git clone https://github.com/apache/nutch.git
3. cd nutch
4. git checkout -b NUTCH-xxx
5. edit files (please try and include a test case if possible)
6. git status (make sure it shows what files you expected to edit)
7. Make sure that your code complies with the [Nutch codeformatting template](https://raw.githubusercontent.com/apache/nutch/master/eclipse-codeformat.xml), which is basially two space indents
8. git add <files>
9. git commit -m “fix for NUTCH-xxx contributed by <your username>”
10. git fork
11. git push -u <your git username> NUTCH-xxx
12. git pull-request
```

# IDE setup

Ensure you have Java 11 Installed. You can see latest versions [here](https://www.oracle.com/java/technologies/downloads/#java11).

Install ANT 1.10.12 (Followed: https://ant.apache.org/manual/install.html)

```bash
cd $ANT_HOME
ant -f fetch.xml -Ddest=system
```

If you use Intellij IDEA, first install the [IvyIDEA Plugin](https://plugins.jetbrains.com/plugin/3612-ivyidea).

Generate the project files

```
cd $WORKSPACE_HOME/external/nutch
ant eclipse
```

### Intellij IDEA

Import the project into IntelliJ. Followed [this video](https://youtu.be/fMwZSTP__Ug).

Open Intellij.

File > New > Project from Existing Sources.

Select the nutch folder.

On next screen select Create Project From Existing Sources. On the next screen, choose the proper location. On the next screen, choose "Create New Project" (vs Import). Go through the rest of the screens. Ensure you select Java 11 for the project.

Once the project is opened, you may see popups like "Ant build scripts found", "Frameworks detected - IvyIDEA Framework detected". Select Accept.


### Eclipse
If you are using Eclipse...
and follow the instructions in [Importing existing projects](https://help.eclipse.org/2019-06/topic/org.eclipse.platform.doc.user/tasks/tasks-importproject.htm).



### Running

You must [configure the nutch-site.xml](https://cwiki.apache.org/confluence/display/NUTCH/RunNutchInEclipse) before running. Make sure, you've added ```http.agent.name``` and ```plugin.folders``` properties. The plugin.folders normally points to ```<project_root>/build/plugins```. 

Now create a Java Application Configuration, choose org.apache.nutch.crawl.Injector, add two paths as arguments. First one is the crawldb directory, second one is the URL directory where, the injector can read urls. Now run your configuration. 

If we still see the ```No plugins found on paths of property plugin.folders="plugins"```, update the plugin.folders in the nutch-default.xml, this is a quick fix, but should not be used.


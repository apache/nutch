Common Crawl Fork of Apache Nutch
=================================

Please also have a look at the [Apache Nutch](/apache/nutch) repository and all information about Apache Nutch given below.

Notable additions in Common Crawl's fork of Nutch (not yet pushed to upstream Nutch although this is planned):
- WARC and CDX writer integrated into Fetcher and able to detect the language of HTML pages using the CLD2 language detector
- [Generator2](src/java/org/apache/nutch/crawl/Generator2.java): alternative implementation of Generator
  - allowing to combine per-domain and per-host limits and
  - optimized to create many (eg. 100) segments in a single job

How to install additional requirements to build this fork of Nutch:
- [crawler-commons](/crawler-commons/crawler-commons) development snapshot package:
  ```
  git clone git@github.com:crawler-commons/crawler-commons.git
  cd crawler-commons/
  mvn install
  ```
- install the latest public suffix list into `conf/` to ensure that it is definitely used (see #17):
  ```
  wget https://publicsuffix.org/list/public_suffix_list.dat -O conf/effective_tld_names.dat
  ```
- [Java wrapper for CLD2 language detection](/commoncrawl/language-detection-cld2)
  ```
  git clone git@github.com:commoncrawl/language-detection-cld2.git
  cd language-detection-cld2/
  mvn install
  ```
  For runtime, if WARC language detection is enabled (`warc.detect.language` = true), also the CLD2 shared objects are required, e.g. on Ubuntu
  ```
  sudo apt install libcld2-0 libcld2-dev
  ```

Apache Nutch
============

<img src="https://nutch.apache.org/assets/img/nutch_logo_tm.png" align="right" width="300" />

For the latest information about Nutch, please visit the Nutch website at:

   https://nutch.apache.org/

and our wiki, at:

   https://cwiki.apache.org/confluence/display/NUTCH/Home

To get started using Nutch read Tutorial:

   https://cwiki.apache.org/confluence/display/NUTCH/NutchTutorial

Contributing
============
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

IDE setup
=========

Generate Eclipse project files

```
ant eclipse
```

and follow the instructions in [Importing existing projects](https://help.eclipse.org/2019-06/topic/org.eclipse.platform.doc.user/tasks/tasks-importproject.htm).

For Intellij IDEA, first install the [IvyIDEA Plugin](https://plugins.jetbrains.com/plugin/3612-ivyidea). then run ```ant eclipse```. 

Then open the project in IntelliJ. You may see popups like "Ant build scripts found", "Frameworks detected - IvyIDEA Framework detected". Just follow the simple steps in these dialogs.  

You must [configure the nutch-site.xml](https://cwiki.apache.org/confluence/display/NUTCH/RunNutchInEclipse) before running. Make sure, you've added ```http.agent.name``` and ```plugin.folders``` properties. The plugin.folders normally points to ```<project_root>/build/plugins```. 

Now create a Java Application Configuration, choose org.apache.nutch.crawl.Injector, add two paths as arguments. First one is the crawldb directory, second one is the URL directory where, the injector can read urls. Now run your configuration. 

If we still see the ```No plugins found on paths of property plugin.folders="plugins"```, update the plugin.folders in the nutch-default.xml, this is a quick fix, but should not be used.


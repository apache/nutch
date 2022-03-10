Apache Nutch README
===================

hello

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


Export Control
==============
This distribution includes cryptographic software.  The country in which you 
currently reside may have restrictions on the import, possession, use, and/or 
re-export to another country, of encryption software.  BEFORE using any encryption 
software, please check your country's laws, regulations and policies concerning the
import, possession, or use, and re-export of encryption software, to see if this is 
permitted.  See <https://www.wassenaar.org/> for more information. 

The U.S. Government Department of Commerce, Bureau of Industry and Security (BIS), has 
classified this software as Export Commodity Control Number (ECCN) 5D002.C.1, which 
includes information security software using or performing cryptographic functions with 
asymmetric algorithms.  The form and manner of this Apache Software Foundation 
distribution makes it eligible for export under the License Exception ENC Technology 
Software Unrestricted (TSU) exception (see the BIS Export Administration Regulations, 
Section 740.13) for both object code and source code.

The following provides more details on the included cryptographic software:

Apache Nutch uses the PDFBox API in its parse-tika plugin for extracting textual content 
and metadata from encrypted PDF files. See https://pdfbox.apache.org/ for more 
details on PDFBox.

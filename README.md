Common Crawl Fork of Apache Nutch
=================================

Please also have a look at the [Apache Nutch](/apache/nutch) master repository and all information about Apache Nutch given below.

Notable additions in Common Crawls fork of Nutch (not yet pushed to upstream Nutch although this is planned):
- WARC and CDX writer which also detects the language of HTML pages using the CLD2 language detector
- Generator2: alternative implementation of Generator allowing to define per-domain and per-host limts and optimized to create many (eg. 100) segments in a single job

How to install additional requirements to build this fork of Nutch:
- [crawler-commons](/crawler-commons/crawler-commons) development snapshot package:
  ```
  git clone git@github.com:crawler-commons/crawler-commons.git
  cd crawler-commons/
  # update to current public suffix list
  wget https://publicsuffix.org/list/public_suffix_list.dat
  mv public_suffix_list.dat src/main/resources/effective_tld_names.dat
  mvn install
  ```
  To ensure that the latest public suffix list is definitely used (see #17):
  ```
  wget https://publicsuffix.org/list/public_suffix_list.dat -O conf/effective_tld_names.dat
  ```
- [Java wrapper for CLD2 language detection](/commoncrawl/language-detection-cld2)
  ```
  git clone git@github.com:commoncrawl/language-detection-cld2.git
  cd language-detection-cld2/
  mvn install
  ```
  For runtime, if WARC language detection is enabled (warc.detect.language = true), also the CLD2 shared objects are required, e.g. on Ubuntu
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

IntelliJ IDEA users can also import Eclipse projects using the ["Eclipser" plugin](https://www.tutorialspoint.com/intellij_idea/intellij_idea_migrating_from_eclipse.htm)https://plugins.jetbrains.com/plugin/7153-eclipser), see also [Importing Eclipse Projects into IntelliJ IDEA](https://www.jetbrains.com/help/idea/migrating-from-eclipse-to-intellij-idea.html#migratingEclipseProject).


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

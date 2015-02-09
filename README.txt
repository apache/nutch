Apache Nutch README

For the latest information about Nutch, please visit our website at:

   http://nutch.apache.org

and our wiki, at:

   http://wiki.apache.org/nutch/

To get started using Nutch read Tutorial:

   http://wiki.apache.org/nutch/NutchTutorial
   
Contributing
============
To contribute a patch, follow these instructions (note that installing
[Hub](http://hub.github.com) is not strictly required, but is recommended).

```
0. Download and install hub.github.com
1. File JIRA issue for your fix at https://issues.apache.org/jira/browse/NUTCH
- you will get issue id NUTCH-xxx where xxx is the issue ID.
2. git clone http://github.com/apache/nutch.git 
3. cd nutch
4. git checkout -b NUTCH-xxx
5. edit files
6. git status (make sure it shows what files you expected to edit)
7. git add <files>
8. git commit -m “fix for NUTCH-xxx contributed by <your username>”
9. git fork
10. git push -u <your git username> NUTCH-xxx
11. git pull-request
```
   
Export Control

This distribution includes cryptographic software.  The country in which you 
currently reside may have restrictions on the import, possession, use, and/or 
re-export to another country, of encryption software.  BEFORE using any encryption 
software, please check your country's laws, regulations and policies concerning the
import, possession, or use, and re-export of encryption software, to see if this is 
permitted.  See <http://www.wassenaar.org/> for more information. 

The U.S. Government Department of Commerce, Bureau of Industry and Security (BIS), has 
classified this software as Export Commodity Control Number (ECCN) 5D002.C.1, which 
includes information security software using or performing cryptographic functions with 
asymmetric algorithms.  The form and manner of this Apache Software Foundation 
distribution makes it eligible for export under the License Exception ENC Technology 
Software Unrestricted (TSU) exception (see the BIS Export Administration Regulations, 
Section 740.13) for both object code and source code.

The following provides more details on the included cryptographic software:

Apache Nutch uses the PDFBox API in its parse-tika plugin for extracting textual content 
and metadata from encrypted PDF files. See http://pdfbox.apache.org for more 
details on PDFBox.

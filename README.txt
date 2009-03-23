Apache Nutch README

Important note: Due to licensing issues we cannot provide two libraries that
are normally provided with PDFBox (jai_core.jar, jai_codec.jar), the parser
library we use for parsing PDF files. If you encounter unexpected problems when
working with PDF files please

1. download the two missing libraries  from:
   http://pdfbox.cvs.sourceforge.net/viewvc/pdfbox/pdfbox/external/

2. Put them to directory src/plugin/parse-pdf/lib
3. follow the instructions in file src/plugin/parse-pdf/plugin.xml
4. Rebuild nutch.



Interesting files include:


  docs/api/index.html
      Javadocs for the Nutch software.

  CHANGES.txt
      Log of changes to Nutch.


For the latest information about Nutch, please visit our website at:

   http://lucene.apache.org/nutch/

and our wiki, at:

   http://wiki.apache.org/nutch/

To get started using Nutch read Tutorial:

   http://lucene.apache.org/nutch/tutorial.html
   
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

Apache Nutch uses the PDFBox API in its parse-pdf plugin for extracting textual content 
and metadata from encrypted PDF files. See http://incubator.apache.org/pdfbox/ for more 
details on PDFBox.

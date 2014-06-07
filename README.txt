Apache Nutch README

For the latest information about Nutch, please visit our website at:

   http://nutch.apache.org

and our wiki, at:

   http://wiki.apache.org/nutch/

To get started using Nutch read Tutorial:

   http://wiki.apache.org/nutch/Nutch2Tutorial
   
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


**- JDK **

    # cd /opt
    # curl -LO "http://download.oracle.com/otn-pub/java/jdk/7u55-b13/jdk-7u55-linux-x64.tar.gz" -H 'Cookie: oraclelicense=accept-securebackup-cookie'
    # tar xzf jdk-7u55-linux-x64.tar.gz
    # mv jdk1.7.0_55 /opt/jdk
    # rm -f jdk-7u55-linux-x64.tar.gz 
    # echo '# JDK' >> /etc/profile
    # echo 'export JAVA_HOME=/opt/jdk' >> /etc/profile
    # echo 'export PATH=$PATH:$JAVA_HOME/bin' >> /etc/profile
    # echo '' >> /etc/profile

**- Maven **

    # curl -LO "http://www.us.apache.org/dist/maven/maven-3/3.2.1/binaries/apache-maven-3.2.1-bin.tar.gz"
    # tar xzf apache-maven-3.2.1-bin.tar.gz
    # rm -f apache-maven-3.2.1-bin.tar.gz
    # echo '# Maven' >> /etc/profile
    # echo 'M2_HOME=/opt/apache-maven-3.2.1' >> /etc/profile
    # echo 'export M2=$M2_HOME/bin' >> /etc/profile
    # echo 'export PATH=$PATH:$M2' >> /etc/profile
    # echo '' >> /etc/profile

**- Ant **

    # curl -LO "http://www.us.apache.org/dist/ant/binaries/apache-ant-1.9.4-bin.tar.bz2"
    # tar xjf apache-ant-1.9.4-bin.tar.bz2
    # rm -f apache-ant-1.9.4-bin.tar.bz2
    # echo '# Ant' >> /etc/profile
    # echo "export ANT_HOME=/opt/apache-ant-1.9.4" >> /etc/profile
    # echo 'export PATH=$PATH:$ANT_HOME/bin' >> /etc/profile
    # echo '' >> /etc/profile

**- Apache Nutch Build **

    # git clone https://github.com/ruo91/nutch
    # cd nutch
    # git checkout -b branch-2.2.1 origin/branch-2.2.1
    # ant

/*
 * ZipTextExtractor.java
 *
 *
 */

package org.apache.nutch.parse.zip;

import java.util.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.net.URL;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParserFactory;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.LogFormatter;

/**
 *
 * @author Rohit Kulkarni & Ashish Vaidya
 */
public class ZipTextExtractor {
	public static final Logger LOG = LogFormatter.getLogger(ZipTextExtractor.class.getName());
    
    /** Creates a new instance of ZipTextExtractor */
    public ZipTextExtractor() {
    }
    
    public String extractText(InputStream input, String url, List outLinksList) throws IOException {
        String resultText = "";
	byte temp;
        
        ZipInputStream zin = new ZipInputStream(input);
        
        ZipEntry entry;
        
        while ((entry = zin.getNextEntry()) != null) {
            
            if (!entry.isDirectory()) {
	    	int size = (int) entry.getSize();
                byte[] b = new byte[size];
                for(int x = 0; x < size; x++) {
			int err = zin.read();
			if(err != -1) {
				b[x] = (byte)err;
			} 
		}
		String newurl = url + "/";
                String fname = entry.getName();
		newurl += fname;
		URL aURL = new URL(newurl);
		String base = aURL.toString();
                int i = fname.lastIndexOf('.');
                if (i != -1) {
                    // file name has extension
                    String contentType = "";
                    String ext = fname.substring(i + 1, fname.length());
                    if (ext.equalsIgnoreCase("txt") || ext.equalsIgnoreCase("c")
                    || ext.equalsIgnoreCase("cc") || ext.equalsIgnoreCase("pl")
                    || ext.equalsIgnoreCase("sh") || ext.equalsIgnoreCase("java")
                    || ext.equalsIgnoreCase("cpp")) {
                        contentType = "text/plain";
                    } else if (ext.equalsIgnoreCase("html") || ext.equalsIgnoreCase("htm")) {
                        contentType = "text/html";
                    } else if (ext.equalsIgnoreCase("xls") || ext.equalsIgnoreCase("xla")
                    || ext.equalsIgnoreCase("xlt") || ext.equalsIgnoreCase("xlw")) {
                        contentType = "application/vnd.ms-excel";
                    } else if (ext.equalsIgnoreCase("ppt") || ext.equalsIgnoreCase("pps")) {
                        contentType = "application/vnd.ms-powerpoint";
                    } else if (ext.equalsIgnoreCase("doc")) {
                        contentType = "application/msword";
                    } else if (ext.equalsIgnoreCase("mp3")) {
                        contentType = "audio/mpeg";
                    } else if (ext.equalsIgnoreCase("pdf")) {
                        contentType = "application/pdf";
                    } else if (ext.equalsIgnoreCase("rtf")) {
                        contentType = "application/rtf";
                    } else if (ext.equalsIgnoreCase("zip")) {
                        contentType = "application/zip";
                    }
		    System.out.println("trying to parse " + fname);
		    try {
                    	Properties metadata = new Properties();
			metadata.setProperty("Content-Length", Long.toString(entry.getSize()));
			metadata.setProperty("Content-Type", contentType);
			Content content = new Content(newurl, base, b, contentType, metadata);
                    	Parser parser = ParserFactory.getParser(contentType, newurl);
                    	Parse parse = parser.getParse(content);
                    	ParseData theParseData = parse.getData();
       			Outlink[] theOutlinks = theParseData.getOutlinks();
			
			for(int count = 0; count < theOutlinks.length; count++) {
				outLinksList.add(new Outlink(theOutlinks[count].getToUrl(), theOutlinks[count].getAnchor()));
			}
			
                    	resultText += entry.getName() + " " + parse.getText() + " ";
		    } catch (ParseException e) {
        
			LOG.info("fetch okay, but can't parse " + fname + ", reason: " + e.getMessage());
		    }
                }
            }
        }
        
	return resultText;
    }
    
}


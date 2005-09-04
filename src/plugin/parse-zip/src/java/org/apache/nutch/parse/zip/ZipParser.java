/*
 * ZipParser.java
 *
 * Nutch parse plugin for zip files - Content Type : application/zip
 */

package org.apache.nutch.parse.zip;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.List;

import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.LogFormatter;

/**
 *
 * @author Rohit Kulkarni & Ashish Vaidya
 * ZipParser class based on MSPowerPointParser class by Stephan Strittmatter
 */
public class ZipParser implements Parser{
    
    private static final Logger LOG = LogFormatter.getLogger(ZipParser.class.getName());
    /** Creates a new instance of ZipParser */
    public ZipParser() {
    }
    
    public Parse getParse(final Content content) {
        
        // check that contentType is one we can handle
        final String contentType = content.getContentType();
        if (contentType != null && !contentType.startsWith("application/zip")) {
            return new ParseStatus(ParseStatus.FAILED, ParseStatus.FAILED_INVALID_FORMAT,
              "Content-Type not application/zip: " + contentType).getEmptyParse();
        }
        
        String resultText = null;
        String resultTitle = null;
        Outlink[] outlinks = null;
        List outLinksList = new ArrayList();
	Properties properties = null;
        
        try {
            final String contentLen = content.get("Content-Length");
            final int len = Integer.parseInt(contentLen);
            System.out.println("ziplen: " + len);
            final byte[] contentInBytes = content.getContent();
            final ByteArrayInputStream bainput = new ByteArrayInputStream(contentInBytes);
            final InputStream input = bainput;
            
            if (contentLen != null && contentInBytes.length != len) {
                return new ParseStatus(ParseStatus.FAILED,
                                       ParseStatus.FAILED_TRUNCATED,
                                       "Content truncated at " + contentInBytes.length +
                                       " bytes. Parser can't handle incomplete pdf file.").getEmptyParse();
            }
            
            ZipTextExtractor extractor = new ZipTextExtractor();
            
            // extract text
            resultText = extractor.extractText(new ByteArrayInputStream(contentInBytes),
	    				content.getUrl(), outLinksList);
            
        } catch (Exception e) {
            return new ParseStatus(ParseStatus.FAILED,
                                   "Can't be handled as Zip document. " + e).getEmptyParse();
        }
        
        // collect meta data
        final Properties metadata = new Properties();
        metadata.putAll(content.getMetadata()); // copy through
        
        if (resultText == null) {
            resultText = "";
        }
        
        if (resultTitle == null) {
            resultTitle = "";
        }
	
        outlinks = (Outlink[])outLinksList.toArray(new Outlink[0]);
        final ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS,
                                                  resultTitle, 
                                                  outlinks, 
                                                  metadata);
        
        LOG.finest("Zip file parsed sucessfully !!");
        return new ParseImpl(resultText, parseData);
    }
    
}

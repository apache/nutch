/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;

/**
 * This class provides methods to map crawled data on JSON using a StringBuilder object.
 * @see <a href='https://docs.oracle.com/javase/7/docs/api/java/lang/StringBuilder.html'>StringBuilder</a> 
 *
 */
public class CommonCrawlFormatSimple extends AbstractCommonCrawlFormat {
	
	private StringBuilder sb;
	
	private int tabCount;
	
	public CommonCrawlFormatSimple(String url, Content content, Metadata metadata, Configuration nutchConf, CommonCrawlConfig config) throws IOException {
		super(url, content, metadata, nutchConf, config);
		
		this.sb = new StringBuilder();
		this.tabCount = 0;
	}
	
	@Override
	protected void writeKeyValue(String key, String value) throws IOException {
		sb.append(printTabs() + "\"" + key + "\": " + quote(value) + ",\n");
	}
	
	@Override
	protected void writeKeyNull(String key) throws IOException {
		sb.append(printTabs() + "\"" + key + "\": null,\n");
	}
	
	@Override
	protected void startArray(String key, boolean nested, boolean newline) throws IOException {
		String name = (key != null) ? "\"" + key + "\": " : "";
		String nl = (newline) ? "\n" : "";
		sb.append(printTabs() + name + "[" + nl);
		if (newline) {
			this.tabCount++;
		}
	}
	
	@Override
	protected void closeArray(String key, boolean nested, boolean newline) throws IOException {
		if (sb.charAt(sb.length()-1) == ',') {
			sb.deleteCharAt(sb.length()-1); // delete comma
		}
		else if (sb.charAt(sb.length()-2) == ',') {
			sb.deleteCharAt(sb.length()-2); // delete comma
		}
		String nl = (newline) ? printTabs() : "";
		if (newline) {
			this.tabCount++;
		}
		sb.append(nl + "],\n");
	}
	
	@Override
	protected void writeArrayValue(String value) {
		sb.append("\"" + value + "\",");
	}
	
	protected void startObject(String key) throws IOException {
		String name = "";
		if (key != null) {
			name = "\"" + key + "\": ";
		}
		sb.append(printTabs() + name + "{\n");
		this.tabCount++;
	}
	
	protected void closeObject(String key) throws IOException {
		if (sb.charAt(sb.length()-2) == ',') {
			sb.deleteCharAt(sb.length()-2); // delete comma
		}
		this.tabCount--;
		sb.append(printTabs() + "},\n");
	}
	
	protected String generateJson() throws IOException {
		sb.deleteCharAt(sb.length()-1); // delete new line
		sb.deleteCharAt(sb.length()-1); // delete comma
		return sb.toString();
	}
	
	private String printTabs() {
		StringBuilder sb = new StringBuilder();
		for (int i=0; i < this.tabCount ;i++) {
			sb.append("\t");
		}
		return sb.toString();
	}
	
    private static String quote(String string) throws IOException {
    	StringBuilder sb = new StringBuilder();
    	
        if (string == null || string.length() == 0) {
            sb.append("\"\"");
            return sb.toString();
        }

        char b;
        char c = 0;
        String hhhh;
        int i;
        int len = string.length();

        sb.append('"');
        for (i = 0; i < len; i += 1) {
            b = c;
            c = string.charAt(i);
            switch (c) {
            case '\\':
            case '"':
                sb.append('\\');
                sb.append(c);
                break;
            case '/':
                if (b == '<') {
                	sb.append('\\');
                }
                sb.append(c);
                break;
            case '\b':
            	sb.append("\\b");
                break;
            case '\t':
            	sb.append("\\t");
                break;
            case '\n':
            	sb.append("\\n");
                break;
            case '\f':
            	sb.append("\\f");
                break;
            case '\r':
            	sb.append("\\r");
                break;
            default:
                if (c < ' ' || (c >= '\u0080' && c < '\u00a0')
                        || (c >= '\u2000' && c < '\u2100')) {
                	sb.append("\\u");
                    hhhh = Integer.toHexString(c);
                    sb.append("0000", 0, 4 - hhhh.length());
                    sb.append(hhhh);
                } else {
                	sb.append(c);
                }
            }
        }
        sb.append('"');
        return sb.toString();
    }
}

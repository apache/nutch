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
package org.apache.nutch.urlfilter.model;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.ModelURLFilterAbstract;


import java.io.Reader;

import java.io.BufferedReader;

import java.io.IOException;

import java.util.ArrayList;

/**
 * Filters URLs based on a file of URL prefixes. The file is named by (1)
 * property "urlfilter.prefix.file" in ./conf/nutch-default.xml, and (2)
 * attribute "file" in plugin.xml of this plugin Attribute "file" has higher
 * precedence if defined.
 * 
 * <p>
 * The format of this file is one URL prefix per line.
 * </p>
 */
public class ModelURLFilter extends ModelURLFilterAbstract {

	private static final Logger LOG = LoggerFactory
			.getLogger(ModelURLFilter.class);

	private boolean relevent = false;
	private Configuration conf;
	private String inputFilePath;
	private String dictionaryFile;
	private ArrayList<String> wordlist = new ArrayList<String>();

	public ModelURLFilter() throws Exception {

	}

	public void configure(String[] args) {

		inputFilePath = args[0];
		dictionaryFile = args[1];
		BufferedReader br = null;

		try {

			String CurrentLine;

			Reader reader = conf.getConfResourceAsReader(dictionaryFile);
			br = new BufferedReader(reader);
			while ((CurrentLine = br.readLine()) != null) {
				wordlist.add(CurrentLine);
			}

		} catch (IOException e) {

			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		try {

			train();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void filterParse(String text) {

		try {
			relevent = classify(text);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public boolean filterUrl(String url) {

		if (!relevent) {
			if (!containsWord(url, wordlist)) {
				return false;
			}
		}

		return true;
	}

	public String filter(String url) {

		return url;

	}

	public boolean classify(String text) throws IOException {

		// if classified as relevent "1" then return true
		if (NBClassifier.classify(text).equals("1"))
			return true;
		return false;
	}

	public void train() throws Exception {

		// check if the model file exists, if it does then don't train
		NBClassifier.createModel(inputFilePath);

	}

	public boolean containsWord(String url, ArrayList<String> wordlist) {
		for (String word : wordlist) {
			if (url.contains(word)) {
				return true;
			}
		}

		return false;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;

	}

	public Configuration getConf() {
		return this.conf;
	}

}
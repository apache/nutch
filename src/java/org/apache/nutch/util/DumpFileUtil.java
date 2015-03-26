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

package org.apache.nutch.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.MD5Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class DumpFileUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DumpFileUtil.class
            .getName());

    private final static String DIR_PATTERN = "%s/%s/%s";
    private final static String FILENAME_PATTERN = "%s_%s.%s";
    private final static Integer MAX_LENGTH_OF_FILENAME = 32;
    private final static Integer MAX_LENGTH_OF_EXTENSION = 5; 
   
    public static String getUrlMD5(String url) {
        byte[] digest = MD5Hash.digest(url).getDigest();

        StringBuffer sb = new StringBuffer();
        for (byte b : digest) {
            sb.append(String.format("%02x", b & 0xff));
        }

        return sb.toString();
    }

    public static String createTwoLevelsDirectory(String basePath, String md5, boolean makeDir) {
        String firstLevelDirName = new StringBuilder().append(md5.charAt(0)).append(md5.charAt(8)).toString();
        String secondLevelDirName = new StringBuilder().append(md5.charAt(16)).append(md5.charAt(24)).toString();

        String fullDirPath = String.format(DIR_PATTERN, basePath, firstLevelDirName, secondLevelDirName);

        if (makeDir) {
	        try {
	            FileUtils.forceMkdir(new File(fullDirPath));
	        } catch (IOException e) {
	            LOG.error("Failed to create dir: {}", fullDirPath);
	            fullDirPath = null;
	        }
        }

        return fullDirPath;
    }
    
    public static String createTwoLevelsDirectory(String basePath, String md5) {
        return createTwoLevelsDirectory(basePath, md5, true);
    }

    public static String createFileName(String md5, String fileBaseName, String fileExtension) {
        if (fileBaseName.length() > MAX_LENGTH_OF_FILENAME) {
            LOG.info("File name is too long. Truncated to {} characters.", MAX_LENGTH_OF_FILENAME);
            fileBaseName = StringUtils.substring(fileBaseName, 0, MAX_LENGTH_OF_FILENAME);
        } 
        
        if (fileExtension.length() > MAX_LENGTH_OF_EXTENSION) {
            LOG.info("File extension is too long. Truncated to {} characters.", MAX_LENGTH_OF_EXTENSION);
            fileExtension = StringUtils.substring(fileExtension, 0, MAX_LENGTH_OF_EXTENSION);
        }

        return String.format(FILENAME_PATTERN, md5, fileBaseName, fileExtension);
    }
}

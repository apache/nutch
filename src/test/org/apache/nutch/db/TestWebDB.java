/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.db;

import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;

import junit.framework.TestCase;
import java.io.*;

/*************************************************
 * This is the unit test for WebDBWriter and 
 * WebDBReader.  It's really not much more than
 * a nice Junit-compatible wrapper around DBTester.
 *
 *************************************************/
public class TestWebDB extends TestCase {

    /**
     * Build the TestWebDB by calling super's constructor.
     */
    public TestWebDB(String name) {
        super(name);
    }

    /**
     * The main test method, invoked by Junit.
     */
    public void testWebDB() throws Exception {
        String testDir = System.getProperty("test.build.data",".");
        File dbDir = new File(testDir, "testDb");
        dbDir.delete();
        dbDir.mkdir();

        DBTester tester = new DBTester(new LocalFileSystem(), dbDir, 500);
        tester.runTest();
        tester.cleanup();
        dbDir.delete();
    }
}

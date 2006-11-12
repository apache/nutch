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
package org.apache.nutch.util.mime;

// JDK imports
import java.net.URL;
import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.InputStreamReader;

// JUnit imports
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;



/**
 * JUnit based tests for class {@link org.apache.nutch.mime.MimeTypes}
 *
 * @author Jerome Charron - http://frutch.free.fr/
 */
public class TestMimeTypes extends TestCase {

    private MimeTypes mimes = null;
    
    public TestMimeTypes(String testName) {
        super(testName);
    }

    public static void main(String[] args) {
        TestRunner.run(suite());
    }
    
    protected void setUp() throws Exception {
        mimes = MimeTypes.get("mime-types.xml");
    }
    
    public static Test suite() {
        return new TestSuite(TestMimeTypes.class);
    }

    
    public void testGetInstance() {
        try {
            MimeTypes mimeTypes = MimeTypes.get("unknown-file");
            assertNull(mimeTypes.getMimeType("filename.html"));
            mimeTypes = MimeTypes.get("mime-types.xml");
            assertNotNull(mimeTypes.getMimeType("filename.html"));
        } catch(Exception e) {
            fail(e.getLocalizedMessage());
        }
    }
    
    /**
     * Test of <code>getMimeType(String)</code>, <code>getMimeType(File)</code>
     * and <code>getMimeType(URL)</code> methods.
     */
    public void testGetMimeFromExtension() {

        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    this.getClass().getResourceAsStream("mime-types.txt")));
            String line = null;
            while((line = in.readLine()) != null) {
                String[] tokens = line.split(";");
                if (tokens[1].equals("rpm")) {
                    // TODO...
                } else if (!tokens[1].equals("")) {
                    MimeType type = new MimeType(tokens[0]);
                    assertEquals(type, mimes.getMimeType("filename." + tokens[1]));
                    assertEquals(type, mimes.getMimeType(new File("filename." + tokens[1])));
                    assertEquals(type, mimes.getMimeType(new URL("http://frutch.free.fr/filename." + tokens[1])));
                }
            }
            in.close();
            assertNull(mimes.getMimeType("filename.not-registered"));
            assertNull(mimes.getMimeType(new File("filename.not-registered")));
            assertNull(mimes.getMimeType(new URL("http://frutch.free.fr/filename.not-registered")));
        } catch(Exception e) {
            fail(e.toString());
        }
    }

    /** Test of <code>getMimeType(byte[])</code> method. */
    public void testGetMimeTypeFromData() {

        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    this.getClass().getResourceAsStream("mime-types.txt")));
            String line = null;
            while((line = in.readLine()) != null) {
                String[] tokens = line.split(";");
                if (tokens.length == 3) {
                    assertEquals(new MimeType(tokens[0]), mimes.getMimeType(getData(tokens[2])));
                }
            }
            in.close();
        } catch(Exception e) {
            fail(e.toString());
        }
    }

    /** Test of <code>getMimeType(String, byte[])</code> method. */
    public void testGetMimeTypeFromDataAndExtension() {

        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    this.getClass().getResourceAsStream("mime-types.txt")));
            String line = null;
            while((line = in.readLine()) != null) {
                byte[] data = null;
                String[] tokens = line.split(";");
                if (tokens[1].equals("rpm")) {
                    // TODO...
                } else {
                    if (tokens.length == 3) {
                        data = getData(tokens[2]);
                    }
                    assertEquals(new MimeType(tokens[0]), mimes.getMimeType("filename." + tokens[1], data));
                }
            }
            in.close();
        } catch(Exception e) {
            fail(e.toString());
        }
    }
    
    private byte[] getData(String filename) throws IOException {
        byte[] data = new byte[mimes.getMinLength()];
        DataInputStream in = new DataInputStream(this.getClass().getResourceAsStream(filename));
        in.read(data);
        in.close();
        return data;
    }
    
    /** Test of <code>add(MimeType)</code> method. */
    public void testAdd() {
        // TODO
    }
    
}

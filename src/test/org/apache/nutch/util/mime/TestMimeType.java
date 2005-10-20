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
package org.apache.nutch.util.mime;

// JUnit imports
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;


/**
 * JUnit based test of class <code>MimeType</code>.
 *
 * @author Jerome Charron - http://frutch.free.fr/
 */
public class TestMimeType extends TestCase {
    
    public TestMimeType(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestMimeType.class);
    }
    
    public static void main(String[] args) {
        TestRunner.run(suite());
    }

    /** Test of <code>MimeType(String)</code> constructor. */
    public void testConstructorString() {
        MimeType type = null;
        constructorFailure(null);
        constructorFailure("");
        constructorFailure("mimetype");
        constructorFailure("mime/type/");
        constructorFailure("/mimetype");
        constructorFailure("mime@type");
        constructorFailure("mime;type");
        type = constructorSuccess("mime/type");
        assertEquals("mime", type.getPrimaryType());
        assertEquals("type", type.getSubType());
        type = constructorSuccess("mime/type;parameter=value");
        assertEquals("mime", type.getPrimaryType());
        assertEquals("type", type.getSubType());
    }

    /** Test of <code>MimeType(String, String)</code> constructor. */
    public void testConstructorStringString() {
        MimeType type = null;
        constructorFailure(null, null);
        constructorFailure("", "");
        constructorFailure("mime", "type/");
        constructorFailure("", "mimetype");
        type = constructorSuccess("mime", "type");
        assertEquals("mime", type.getPrimaryType());
        assertEquals("type", type.getSubType());
        type = constructorSuccess("mime", "type;parameter=value");
        assertEquals("mime", type.getPrimaryType());
        assertEquals("type", type.getSubType());
    }
    
    /** Test of <code>getName</code> method. */
    public void testGetName() {
        constructorFailure(null, null);
        constructorFailure(null, "type");
        constructorFailure("mime", null);
        constructorFailure("", "");
        constructorFailure("mime", "");
        constructorFailure("", "type");
        constructorFailure("mi/me", "type");
        constructorFailure("mime", "/type/");
        constructorSuccess("mime/type");
    }

    /** Test of <code>clean(String)</code> method. */
    public void testClean() {
        try {
            assertEquals("text/html", MimeType.clean("text/html"));
            assertEquals("text/html", MimeType.clean("text/html; charset=ISO-8859-1"));
        } catch (Exception e) {
            fail(e.toString());
        }
        cleanExceptionChecker(null);
        cleanExceptionChecker("");
        cleanExceptionChecker("text");
        cleanExceptionChecker("/html");
        cleanExceptionChecker("/text/html");
    }

    private static void cleanExceptionChecker(String type) {
        try {
            MimeType.clean(type);
            fail("Must raise a MimeTypeException for [" + type + "]");
        } catch (MimeTypeException mte) { // All is ok
        } catch (Exception e) {
            fail("Must raise a MimeTypeException for [" + type + "]");
        }
    }

    /** Test of <code>getPrimaryType</code> method. */
    public void testGetPrimaryType() {
    }

    /** Test of <code>getSubType</code> method. */
    public void testGetSubType() {
    }

    /** Test of <code>toString</code> method. */
    public void testToString() {
    }

    /** Test of <code>equals</code> method. */
    public void testEquals() {
    }

    /** Test of <code>hashCode</code> method. */
    public void testHashCode() {
    }

    /** Test of <code>getDescription</code> method. */
    public void testGetDescription() {
    }

    /**
     * Test of <code>setDescription</code> method. */
    public void testSetDescription() {
    }

    /** Test of <code>addExtension</code> method. */
    public void testAddExtension() {
    }

    /** Test of <code>getExtensions</code> method. */
    public void testGetExtensions() {
    }

    /** Test of <code>addMagic</code> method. */
    public void testAddMagic() {
    }

    /** Test of <code>getMinLength</code> method */
    public void testGetMinLength() {
    }

    /** Test of <code>hasMagic</code> method */
    public void testHasMagic() {
    }

    /** Test of <code>matches</code> method. */
    public void testMatches() {
    }
    

    private void constructorFailure(String str) {
        MimeType type = null;
        try {
            type = new MimeType(str);
            fail("Must Raise a MimeTypeException!");
        } catch (MimeTypeException mte) {
            // All is ok
        }
    }

    private MimeType constructorSuccess(String str) {
        MimeType type = null;
        try {
            type = new MimeType(str);
        } catch (MimeTypeException mte) {
            fail(mte.getLocalizedMessage());
        }
        return type;
    }

    private void constructorFailure(String prim, String sub) {
        MimeType type = null;
        try {
            type = new MimeType(prim, sub);
            fail("Must Raise a MimeTypeException!");
        } catch (MimeTypeException mte) {
            // All is ok
        }
    }

    private MimeType constructorSuccess(String prim, String sub) {
        MimeType type = null;
        try {
            type = new MimeType(prim, sub);
        } catch (MimeTypeException mte) {
            fail(mte.getLocalizedMessage());
        }
        return type;
    }
    
}

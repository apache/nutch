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

// JDK imports
import java.util.ArrayList;


/**
 * Defines a Mime Content Type.
 *
 * @author Jerome Charron - http://frutch.free.fr/
 * @author Hari Kodungallur
 */
public final class MimeType {

    /** The primary and sub types separator */
    private final static String SEPARATOR = "/";
    
    /** The parameters separator */
    private final static String PARAMS_SEP = ";";
    
    /** Special characters not allowed in content types. */
    private final static String SPECIALS = "()<>@,;:\\\"/[]?=";
    
    
    /** The Mime-Type full name */
    private String name = null;

    /** The Mime-Type primary type */
    private String primary = null;

    /** The Mime-Type sub type */
    private String sub = null;

    /** The Mime-Type description */
    private String description = null;
    
    /** The Mime-Type associated extensions */
    private ArrayList extensions = null;
    
    /** The magic bytes associated to this Mime-Type */
    private ArrayList magics = null;
    
    /** The minimum length of data to provides for magic analyzis */
    private int minLength = 0;
    
        
    /**
     * Creates a MimeType from a String.
     * @param name the MIME content type String.
     */
     public MimeType(String name) throws MimeTypeException {
        
        if (name == null || name.length() <= 0) {
            throw new MimeTypeException("The type can not be null or empty");
        }
        
        // Split the two parts of the Mime Content Type
        String[] parts = name.split(SEPARATOR, 2);
        
        // Checks validity of the parts
        if (parts.length != 2) {
            throw new MimeTypeException("Invalid Content Type " + name);
        }
        init(parts[0], parts[1]);
     }    
    
    /**
     * Creates a MimeType with the given primary type and sub type.
     * @param primary the content type primary type.
     * @param sub the content type sub type.
     */
    public MimeType(String primary, String sub) throws MimeTypeException {
        init(primary, sub);
    }
    
    /** Init method used by constructors. */
    private void init(String primary, String sub) throws MimeTypeException {

        // Preliminary checks...
        if ((primary == null) || (primary.length() <= 0) || (!isValid(primary))) {
            throw new MimeTypeException("Invalid Primary Type " + primary);
        }
        // Remove optional parameters from the sub type
        String clearedSub = null;
        if (sub != null) {
            clearedSub = sub.split(PARAMS_SEP)[0];
        }
        if ((clearedSub == null) || (clearedSub.length() <= 0) || (!isValid(clearedSub))) {
            throw new MimeTypeException("Invalid Sub Type " + clearedSub);
        }
                
        // All is ok, assign values
        this.name = primary + SEPARATOR + clearedSub;
        this.primary = primary;
        this.sub = clearedSub;
        this.extensions = new ArrayList();
        this.magics = new ArrayList();
    }

    /**
     * Cleans a content-type.
     * This method cleans a content-type by removing its optional parameters
     * and returning only its <code>primary-type/sub-type</code>.
     * @param type is the content-type to clean.
     * @return the cleaned version of the specified content-type.
     * @throws MimeTypeException if something wrong occurs during the
     *         parsing/cleaning of the specified type.
     */
    public final static String clean(String type) throws MimeTypeException {
        return (new MimeType(type)).getName();
    }


    /**
     * Return the name of this mime-type.
     * @return the name of this mime-type.
     */
    public String getName() {
        return name;
    }

    /**
     * Return the primary type of this mime-type.
     * @return the primary type of this mime-type.
     */
    public String getPrimaryType() {
        return primary;
    }

    /**
     * Return the sub type of this mime-type.
     * @return the sub type of this mime-type.
     */
    public String getSubType() {
        return sub;
    }

    // Inherited Javadoc
    public String toString() {
        return getName();
    }

    /**
     * Indicates if an object is equal to this mime-type.
     * The specified object is equal to this mime-type if it is not null, and
     * it is an instance of MimeType and its name is equals to this mime-type.
     *
     * @param object the reference object with which to compare.
     * @return <code>true</code> if this mime-type is equal to the object
     *         argument; <code>false</code> otherwise.
     */
    public boolean equals(Object object) {
        try {
            return ((MimeType) object).getName().equals(this.name);
        } catch (Exception e) {
            return false;
        }
    }
    
    // Inherited Javadoc
    public int hashCode() {
        return name.hashCode();
    }
    
    
    /**
     * Return the description of this mime-type.
     * @return the description of this mime-type.
     */
    String getDescription() {
        return description;
    }

    /**
     * Set the description of this mime-type.
     * @param description the description of this mime-type.
     */
    void setDescription(String description) {
        this.description = description;
    }
    
    /**
     * Add a supported extension.
     * @param the extension to add to the list of extensions associated
     *        to this mime-type.
     */
    void addExtension(String ext) {
        extensions.add(ext);
    }

    /**
     * Return the extensions of this mime-type
     * @return the extensions associated to this mime-type.
     */
    String[] getExtensions() {
        return (String[]) extensions.toArray(new String[extensions.size()]);
    }
    
    void addMagic(int offset, String type, String magic) {
        // Some preliminary checks...
        if ((magic == null) || (magic.length() < 1)) {
            return;
        }
        Magic m = new Magic(offset, type, magic);
        if (m != null) {
            magics.add(m);
            minLength = Math.max(minLength, m.size());
        }
    }
    
    int getMinLength() {
        return minLength;
    }
    
    boolean hasMagic() {
        return (magics.size() > 0);
    }
    
    boolean matches(byte[] data) {
        if (!hasMagic()) { return false; }
        
        Magic tested = null;
        for (int i=0; i<magics.size(); i++) {
            tested = (Magic) magics.get(i);
            if (tested.matches(data)) {
                return true;
            }
        }
        return false;
    }

    
    /** Checks if the specified primary or sub type is valid. */
    private boolean isValid(String type) {
        return    (type != null)
               && (type.trim().length() > 0)
               && !hasCtrlOrSpecials(type);
    }

    /** Checks if the specified string contains some special characters. */
    private boolean hasCtrlOrSpecials(String type) {
        int len = type.length();
        int i = 0;
        while (i < len) {
            char c = type.charAt(i);
            if (c <= '\032' || SPECIALS.indexOf(c) > 0) {
            	return true;
            }
            i++;
        }
        return false;
    }

    
    private class Magic {
        
        private int offset;
        private byte[] magic = null;

        Magic(int offset, String type, String magic) {
            this.offset = offset;

            if ((type != null) && (type.equals("byte"))) {
                this.magic = readBytes(magic);
            } else {
                this.magic = magic.getBytes();
            }
        }
        
        int size() {
            return (offset + magic.length);
        }
        
        boolean matches(byte[] data) {
            if (data == null) { return false; }
            
            int idx = offset;
            if ((idx + magic.length) > data.length) {
                return false;
            }

            for (int i=0; i<magic.length; i++) {
                if (magic[i] != data[idx++]) {
                    return false;
                }
            }
            return true;            
        }
        
        private byte[] readBytes(String magic) {
            byte[] data = null;

            if ((magic.length() % 2) == 0) {
                String tmp = magic.toLowerCase();
                data = new byte[tmp.length() / 2];
                int byteValue = 0;
                for (int i=0; i<tmp.length(); i++) {
                    char c = tmp.charAt(i);
                    int number;
                    if (c >= '0' && c <= '9') {
                        number = c - '0';
                    } else if (c >= 'a' && c <= 'f') {
                        number = 10 + c - 'a';
                    } else {
                        throw new IllegalArgumentException();
                    }
                    if ((i % 2) == 0) {
                        byteValue = number * 16;
                    } else {
                        byteValue += number;
                        data[i/2] = (byte) byteValue;
                    }
                }
            }
            return data;
        }
        
        public String toString() {
            StringBuffer buf = new StringBuffer();
            buf.append("[").append(offset)
               .append("/").append(magic).append("]");
            return buf.toString();
        }
    }

}

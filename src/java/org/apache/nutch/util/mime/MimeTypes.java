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
import java.io.File;
import java.net.URL;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;

// Commons Logging imports
import org.apache.commons.logging.Log;


/**
 * This class is a MimeType repository.
 * It gathers a set of MimeTypes and enables to retrieves a content-type
 * from a specified file extension, or from a magic character sequence (or both).
 *
 * @author Jerome Charron - http://frutch.free.fr/
 */
public final class MimeTypes {
    
    /** The default <code>application/octet-stream</code> MimeType */
    public final static String DEFAULT = "application/octet-stream";

    /** All the registered MimeTypes */
    private ArrayList types = new ArrayList();

    /** All the registered MimeType indexed by name */
    private HashMap typesIdx = new HashMap();

    /** MimeTypes indexed on the file extension */
    private Map extIdx = new HashMap();

    /** List of MimeTypes containing a magic char sequence */
    private List magicsIdx = new ArrayList();

    /** The minimum length of data to provide to check all MimeTypes */
    private int minLength = 0;

    /**
     * My registered instances
     * There is one instance associated for each specified file while
     * calling the {@link #get(String)} method.
     * Key is the specified file path in the {@link #get(String)} method.
     * Value is the associated MimeType instance.
     */
    private static Map instances = new HashMap();
    
    
    /** Should never be instanciated from outside */
    private MimeTypes(String filepath, Log logger) {
        MimeTypesReader reader = new MimeTypesReader(logger);
        add(reader.read(filepath));
    }


    /**
     * Return a MimeTypes instance.
     * @param filepath is the mime-types definitions xml file.
     * @return A MimeTypes instance for the specified filepath xml file.
     */
    public static MimeTypes get(String filepath) {
        MimeTypes instance = null;
        synchronized(instances) {
            instance = (MimeTypes) instances.get(filepath);
            if (instance == null) {
                instance = new MimeTypes(filepath, null);
                instances.put(filepath, instance);
            }
        }
        return instance;
    }

    /**
     * Return a MimeTypes instance.
     * @param filepath is the mime-types definitions xml file.
     * @param logger is it Logger to uses for ouput messages.
     * @return A MimeTypes instance for the specified filepath xml file.
     */
    public static MimeTypes get(String filepath, Log logger) {
        MimeTypes instance = null;
        synchronized(instances) {
            instance = (MimeTypes) instances.get(filepath);
            if (instance == null) {
                instance = new MimeTypes(filepath, logger);
                instances.put(filepath, instance);
            }
        }
        return instance;
    }
    
    /**
     * Find the Mime Content Type of a file.
     * @param file to analyze.
     * @return the Mime Content Type of the specified file, or
     *         <code>null</code> if none is found.
     */
    public MimeType getMimeType(File file) {
        return getMimeType(file.getName());
    }

    /**
     * Find the Mime Content Type of a document from its URL.
     * @param url of the document to analyze.
     * @return the Mime Content Type of the specified document URL, or
     *         <code>null</code> if none is found.
     */
    public MimeType getMimeType(URL url) {
       return getMimeType(url.getPath());
    }

    /**
     * Find the Mime Content Type of a document from its name.
     * @param name of the document to analyze.
     * @return the Mime Content Type of the specified document name, or
     *         <code>null</code> if none is found.
     */
    public MimeType getMimeType(String name) {
        MimeType[] founds = getMimeTypes(name);
        if ((founds == null) || (founds.length <1)) {
            // No mapping found, just return null
            return null;
        } else {
            // Arbitraly returns the first mapping
            return founds[0];
        }
    }
    
    /**
     * Find the Mime Content Type of a stream from its content.
     *
     * @param data are the first bytes of data of the content to analyze.
     *        Depending on the length of provided data, all known MimeTypes are
     *        checked. If the length of provided data is greater or egals to
     *        the value returned by {@link #getMinLength()}, then all known
     *        MimeTypes are checked, otherwise only the MimeTypes that could be
     *        analyzed with the length of provided data are analyzed.
     *
     * @return The Mime Content Type found for the specified data, or
     *         <code>null</code> if none is found.
     * @see #getMinLength()
     */
    public MimeType getMimeType(byte[] data) {
        // Preliminary checks
        if ((data == null) || (data.length < 1)) {
            return null;
        }
        Iterator iter = magicsIdx.iterator();
        MimeType type = null;
        // TODO: This is a very naive first approach (scanning all the magic
        //       bytes since one is matching.
        //       A first improvement could be to use a search path on the magic
        //       bytes.
        // TODO: A second improvement could be to search for the most qualified
        //       (the longuest) magic sequence (not the first that is matching).
        while (iter.hasNext()) {
            type = (MimeType) iter.next();
            if (type.matches(data)) {
                return type;
            }
        }
        return null;
    }

    /**
     * Find the Mime Content Type of a document from its name and its content.
     *
     * @param name of the document to analyze.
     * @param data are the first bytes of the document's content.
     * @return the Mime Content Type of the specified document, or
     *         <code>null</code> if none is found.
     * @see #getMinLength()
     */
    public MimeType getMimeType(String name, byte[] data) {
        
        // First, try to get the mime-type from the name
        MimeType mimeType = null;
        MimeType[] mimeTypes = getMimeTypes(name);
        if (mimeTypes == null) {
            // No mime-type found, so trying to analyse the content
            mimeType = getMimeType(data);
        } else if (mimeTypes.length > 1) {
            // TODO: More than one mime-type found, so trying magic resolution
            // on these mime types
            //mimeType = getMimeType(data, mimeTypes);
            // For now, just get the first one
            mimeType = mimeTypes[0];
        } else {
            mimeType = mimeTypes[0];
        }
        return mimeType;
    }
   
   /**
    * Return a MimeType from its name.
    */
   public MimeType forName(String name) {
      return (MimeType) typesIdx.get(name);
   }

    /**
     * Return the minimum length of data to provide to analyzing methods
     * based on the document's content in order to check all the known
     * MimeTypes.
     * @return the minimum length of data to provide.
     * @see #getMimeType(byte[])
     * @see #getMimeType(String, byte[])
     */
    public int getMinLength() {
        return minLength;
    }
    
    
    /**
     * Add the specified mime-types in the repository.
     * @param types are the mime-types to add.
     */
    void add(MimeType[] types) {
        if (types == null) { return; }
        for (int i=0; i<types.length; i++) {
            add(types[i]);
        }
    }
    
    /**
     * Add the specified mime-type in the repository.
     * @param type is the mime-type to add.
     */
    void add(MimeType type) {
        typesIdx.put(type.getName(), type);
        types.add(type);
        // Update minLentgth
        minLength = Math.max(minLength, type.getMinLength());
        // Update the extensions index...
        String[] exts = type.getExtensions();
        if (exts != null) {
            for (int i=0; i<exts.length; i++) {
                List list = (List) extIdx.get(exts[i]);
                if (list == null) {
                    // No type already registered for this extension...
                    // So, create a list of types
                    list = new ArrayList();
                    extIdx.put(exts[i], list);
                }
                list.add(type);
            }
        }
        // Update the magics index...
        if (type.hasMagic()) {
            magicsIdx.add(type);
        }
    }

    /**
     * Returns an array of matching MimeTypes from the specified name
     * (many MimeTypes can have the same registered extensions).
     */
    private MimeType[] getMimeTypes(String name) {
        List mimeTypes = null;
        int index = name.lastIndexOf('.');
        if ((index != -1) && (index != name.length()-1)) {
            // There's an extension, so try to find
            // the corresponding mime-types
            String ext = name.substring(index + 1);
            mimeTypes = (List) extIdx.get(ext);
        }
        
        return (mimeTypes != null)
                    ? (MimeType[]) mimeTypes.toArray(new MimeType[mimeTypes.size()])
                    : null;
    }
    
}

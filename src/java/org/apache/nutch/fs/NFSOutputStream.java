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
package org.apache.nutch.fs;

import java.io.*;

/****************************************************************
 * NFSOutputStream is an OutputStream that can track its position.
 *
 * @author Mike Cafarella
 *****************************************************************/
public abstract class NFSOutputStream extends OutputStream {
    /**
     * Return the current offset from the start of the file
     */
    public abstract long getPos() throws IOException;
}

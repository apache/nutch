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

package org.apache.nutch.ndfs;

import java.io.*;
import org.apache.nutch.io.*;

/**********************************************************************
 * Protocol that an NDFS datanode uses to communicate with the NameNode.
 * It's used to upload current load information and block records.
 *
 * @author Michael Cafarella
 **********************************************************************/
public interface DatanodeProtocol {

    public void sendHeartbeat(String sender, long capacity, long remaining) throws IOException;
    public Block[] blockReport(String sender, Block blocks[]) throws IOException;
    public void blockReceived(String sender, Block blocks[]) throws IOException;
    public void errorReport(String sender, String msg) throws IOException;

    public BlockCommand getBlockwork(String sender, int xmitsInProgress) throws IOException;
}

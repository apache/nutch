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

package org.apache.nutch.io;

import java.io.*;
import java.util.Random;
import junit.framework.TestCase;
import org.apache.nutch.io.*;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.util.NutchConf;

/** Unit tests for Writable. */
public class TestWritable extends TestCase {
  public TestWritable(String name) { super(name); }

  /** Example class used in test cases below. */
  public static class SimpleWritable implements Writable {
    private static final Random RANDOM = new Random();

    int state = RANDOM.nextInt();

    public void write(DataOutput out) throws IOException {
      out.writeInt(state);
    }

    public void readFields(DataInput in) throws IOException {
      this.state = in.readInt();
    }

    public static SimpleWritable read(DataInput in) throws IOException {
      SimpleWritable result = new SimpleWritable();
      result.readFields(in);
      return result;
    }

    /** Required by test code, below. */
    public boolean equals(Object o) {
      if (!(o instanceof SimpleWritable))
        return false;
      SimpleWritable other = (SimpleWritable)o;
      return this.state == other.state;
    }
  }

  /** Test 1: Check that SimpleWritable. */
  public void testSimpleWritable() throws Exception {
    testWritable(new SimpleWritable());
  }

  /** Utility method for testing writables. */
  public static void testWritable(Writable before) throws Exception {
      DataOutputBuffer dob = new DataOutputBuffer();
      before.write(dob);

      DataInputBuffer dib = new DataInputBuffer();
      dib.reset(dob.getData(), dob.getLength());
    
      Writable after = (Writable)before.getClass().newInstance();
      if(after instanceof ParseData) {
        ParseData parseData = (ParseData) after;
        parseData.setConf(new NutchConf());
      }
      after.readFields(dib);

      assertEquals(before, after);
  }
	
}

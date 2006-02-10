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
package org.apache.nutch.parse.msexcel;

// JDK imports
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

// Jakarta POI imports
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.eventfilesystem.POIFSReader;

/**
 * Excel Text and Properties extractor.
 *
 * @author Rohit Kulkarni & Ashish Vaidya
 * @author J&eacute;r&ocirc;me Charron
 */
public class ExcelExtractor {

  
  public String extractText(InputStream input) throws IOException {
    
    String resultText = "";
    HSSFWorkbook wb = new HSSFWorkbook(input);
    if (wb == null) {
      return resultText;
    }
    
    HSSFSheet sheet;
    HSSFRow row;
    HSSFCell cell;
    int sNum = 0;
    int rNum = 0;
    int cNum = 0;
    
    sNum = wb.getNumberOfSheets();
    
    for (int i=0; i<sNum; i++) {
      if ((sheet = wb.getSheetAt(i)) == null) {
        continue;
      }
      rNum = sheet.getLastRowNum();
      for (int j=0; j<=rNum; j++) {
        if ((row = sheet.getRow(j)) == null){
          continue;
        }
        cNum = row.getLastCellNum();
        
        for (int k=0; k<cNum; k++) {
          if ((cell = row.getCell((short) k)) != null) {
            /*if(HSSFDateUtil.isCellDateFormatted(cell) == true) {
                resultText += cell.getDateCellValue().toString() + " ";
              } else
             */
            if (cell.getCellType() == HSSFCell.CELL_TYPE_STRING) {
              resultText += cell.getStringCellValue() + " ";
            } else if (cell.getCellType() == HSSFCell.CELL_TYPE_NUMERIC) {
              Double d = new Double(cell.getNumericCellValue());
              resultText += d.toString() + " ";
            }
            /* else if(cell.getCellType() == HSSFCell.CELL_TYPE_FORMULA){
                 resultText += cell.getCellFormula() + " ";
               } 
             */
          }
        }
      }
    }
    return resultText;
  }
  
  
  public Properties extractProperties(InputStream input) throws IOException {
    
    PropertiesBroker propertiesBroker = new PropertiesBroker();
    POIFSReader reader = new POIFSReader();
    reader.registerListener(new PropertiesReaderListener(propertiesBroker),
                            "\005SummaryInformation");
    reader.read(input);
    return propertiesBroker.getProperties();
  }
  
  
  class PropertiesBroker {
    
    private Properties properties;
    private int timeoutMillis = 2 * 1000;
    
    
    public synchronized Properties getProperties() {
      
      long start = new Date().getTime();
      long now = start;
      
      while ((properties == null) && (now-start < timeoutMillis)) {
        try {
          wait(timeoutMillis / 10);
        } catch (InterruptedException e) {}
        now = new Date().getTime();
      }
      
      notifyAll();
      return properties;
    }
    
    public synchronized void setProperties(Properties properties) {
      this.properties = properties;
      notifyAll();
    }
  }

}


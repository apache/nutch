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
package org.apache.nutch.parse.msexcel;

// JDK imports
import java.io.InputStream;

// Jakarta POI imports
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

// Nutch imports
import org.apache.nutch.parse.ms.MSExtractor;


/**
 * Excel Text and Properties extractor.
 *
 * @author Rohit Kulkarni & Ashish Vaidya
 * @author J&eacute;r&ocirc;me Charron
 */
class ExcelExtractor extends MSExtractor {

  
  protected String extractText(InputStream input) throws Exception {
    
    StringBuilder resultText = new StringBuilder();
    HSSFWorkbook wb = new HSSFWorkbook(input);
    if (wb == null) {
      return resultText.toString();
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
                resultText.append(cell.getDateCellValue().toString())
              } else
             */
            if (cell.getCellType() == HSSFCell.CELL_TYPE_STRING) {
              resultText.append(cell.getStringCellValue()).append(" ");
            } else if (cell.getCellType() == HSSFCell.CELL_TYPE_NUMERIC) {
              double d = cell.getNumericCellValue();
              resultText.append(d).append(" ");
            }
            /* else if(cell.getCellType() == HSSFCell.CELL_TYPE_FORMULA){
                 resultText.append(cell.getCellFormula());
               } 
             */
          }
        }
      }
    }
    return resultText.toString();
  }
  
}

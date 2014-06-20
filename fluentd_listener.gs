/*
Copyright 2014 Google Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//
// fluent_listerner.gs: receives fluentd event log via https and generate a sheet with a chart
//

// SPREADSHEET URL
SPREADSHEET_URL = "<<PLEASE PUT YOUR SPREADSHEET URL HERE>>";

// CONSTS
MAX_ROWS_LARGE = 100; // number of max rows for AREA, LINE, SCATTER and TABLE
MAX_ROWS_SMALL = 5;  // number of max rows for BAR and COLUMN
CHART_WIDTH = 600; // width of each chart in pixel
CHART_HEIGHT = 300; // height of each chart in pixel
CHARTS_PER_ROW = 2; // number of charts in a row
LOG_SHEET_NAME = "logs"; // sheet name used when the event log doesn't include "tag" field

// receiving events from fluent-plugin-https-json
function doPost(e){
  
  // for testing
  if (!e) {
    e = {"parameters":
         {"events":
          '[{"tag": "test_LINE","time": "1","record": {"value1": "10", "value2": "20"}},{"tag": "test_LINE","time": "2","record": {"value1": "15", "value2": "22"}}]'
         }
        }
  }
  
  // process each event
  var events = JSON.parse(e.parameters.events);
  for each (var event in events) {
    processEvent(event);
  }

  // response with empty string
  return ContentService.createTextOutput("");
}

// process each event
function processEvent(event) {
  
  // extract fluentd tag and timestamp
  if (!event.record) return;
  var tag = event.tag;
  delete event.tag;
  var timestamp = new Date(Number(event.time) * 1000);
  delete event.time;

  // get or insert sheet
  var fields = Object.getOwnPropertyNames(event.record).sort();
  var sheet = getOrInsertSheetByTag(tag, fields.length + 1);

  // add new row and delete the last row
  sheet.insertRows(2);
  sheet.deleteRow(sheet.getMaxRows());
  
  // set timestamp
  sheet.getRange(2, 1).setValue(timestamp);
  sheet.getRange(2, 1).setNumberFormat("M/dd HH:mm:ss");
  
  // insert new values
  for (i = 0; i < fields.length; i++) {
    sheet.getRange(1, i + 2).setValue(fields[i]);
    var val = event.record[fields[i]];
    if (val && val.length == 0) val = 0;
    sheet.getRange(2, i + 2).setValue(val);
  }
}

// get or insert sheet
function getOrInsertSheetByTag(tag, colSize) {
  
  // determine sheet name from the tag name
  if (tag == "") {
    tag = LOG_SHEET_NAME;
  }
  var tableSheetName = tag;
  
  // determine chart properties from the tag suffixes
  var chartType = null;
  var startColumn = 1;
  var rowSize = MAX_ROWS_LARGE;
  var isStacked = false;
  var matched = tag.match(/(.*)_(AREA|BAR|COLUMN|LINE|SCATTER|TABLE)(_STACKED)?/);
  if (matched) {
    tableSheetName = matched[1];
    isStacked = matched[3] != null;
    if (matched[2] == "AREA") {
      chartType = Charts.ChartType.AREA;
    } else if (matched[2] == "BAR") {
      chartType = Charts.ChartType.BAR;
      rowSize = MAX_ROWS_SMALL;
    } else if (matched[2] == "COLUMN") {
      chartType = Charts.ChartType.COLUMN;
      rowSize = MAX_ROWS_SMALL;
    } else if (matched[2] == "LINE") {
      chartType = Charts.ChartType.LINE;
    } else if (matched[2] == "SCATTER") {
      chartType = Charts.ChartType.SCATTER;
    } else if (matched[2] == "TABLE") {
      chartType = Charts.ChartType.TABLE;
    }
  }
  
  // check if there's existing table sheet
  var sheets = SpreadsheetApp.openByUrl(SPREADSHEET_URL);
  var tableSheet = sheets.getSheetByName(tableSheetName);
  if (tableSheet) {
    return tableSheet;
  }
  
  // insert new sheet for table
  tableSheet = sheets.insertSheet(tableSheetName);
  tableSheet.insertRows(1, rowSize + 1);
  if (tableSheet.getMaxRows() > rowSize + 1) {
    tableSheet.deleteRows(rowSize + 1, tableSheet.getMaxRows() - rowSize - 1);
  }
//  tableSheet.getRange(1, 1).setValue("timestamp");
  
  // if there's no need for chart, return
  if (!matched) return tableSheet;
  
  // determine position of the new chart
  var firstSheet = sheets.getSheets()[0];
  var numCharts = firstSheet.getCharts().length;
  var posX = (numCharts % CHARTS_PER_ROW) * CHART_WIDTH;
  var posY = Math.floor(numCharts / CHARTS_PER_ROW) * CHART_HEIGHT;
  
  // insert new chart to the firstSheet
  var chart = firstSheet.newChart()
  .setChartType(chartType)
  .addRange(tableSheet.getRange(1, startColumn, rowSize + 1, colSize))
  .setPosition(1, 1, posX, posY)
  .setOption("width", CHART_WIDTH)
  .setOption("height", CHART_HEIGHT)
  .setOption("isStacked", isStacked)
  .setOption("title", tableSheetName)
  .build();
  firstSheet.insertChart(chart);
  return tableSheet;
}

// for testing
function doGet(e) {
  return doPost(e);
}


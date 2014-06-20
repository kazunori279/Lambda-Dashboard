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
// bq_query.gs: executes BigQuery queries and generate a sheet with a chart
//

// GCP PROJECT ID
PROJECT_ID = "<<PLEASE PUT YOUR PROJECT ID HERE>>";

// BQ CONSTS
BQ_SHEET_NAME = "BQ Queries"; // sheet name used to find BQ queries
BQ_COL_QUERYNAME = 1; // column position for query name
BQ_COL_INTERVAL = 2; // column position for interval
BQ_COL_QUERY = 3; // column position for query
BQ_COL_LASTTIME = 4; // column position for last execution time

// run BQ queries on the sheet periodically
function runQueries(isManual) {
  
  // get bqSheet
  var sheets = SpreadsheetApp.openByUrl(SPREADSHEET_URL);
  var bqSheet = sheets.getSheetByName(BQ_SHEET_NAME);
  
  // execute each query
  var rowIndex = 2;
  while(isQueryAvailableAt(bqSheet, rowIndex)) {
    if (isManual == true || isTimeToExecute(bqSheet, rowIndex)) {
      
      // execute the query
      runSingleQuery(bqSheet.getRange(rowIndex, BQ_COL_QUERYNAME).getValue(), bqSheet.getRange(rowIndex, BQ_COL_QUERY).getValue());
      
      // update timestamp
      bqSheet.getRange(rowIndex, BQ_COL_LASTTIME).setValue(new Date());
      bqSheet.getRange(rowIndex, BQ_COL_LASTTIME).setNumberFormat("M/dd HH:mm:ss");      
    }
    rowIndex += 1;
  }
}

// check if query is available at specified row
function isQueryAvailableAt(bqSheet, rowIndex) {
  var queryName = bqSheet.getRange(rowIndex, BQ_COL_QUERYNAME).getValue();
  var queryInterval = bqSheet.getRange(rowIndex, BQ_COL_INTERVAL).getValue();
  var query = bqSheet.getRange(rowIndex, BQ_COL_QUERY).getValue();
  return queryName && queryInterval != null && query;
}

// check if it's time to execute the query at specified row
function isTimeToExecute(bqSheet, rowIndex) {
  var interval = Number(bqSheet.getRange(rowIndex, BQ_COL_INTERVAL).getValue()) * 1000;
  var lastTime = bqSheet.getRange(rowIndex, BQ_COL_LASTTIME).getValue();
  return !lastTime || ((interval > 0) && Number(new Date()) - lastTime > interval * 60);
}

// execute a BQ query
function runSingleQuery(queryName, querySql) {

  // execute the query
  var queryResults = BigQuery.Jobs.query({ "query": querySql }, PROJECT_ID);
  var jobId = queryResults.jobReference.jobId;

  // check on status of the query job.
  var sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(PROJECT_ID, jobId);
  }
  var rows = queryResults.rows;
  if (!rows || rows.length == 0) {
    Logger.log("No Results");
    return;
  }
  
  // get all the rows of results
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(PROJECT_ID, jobId, {
      pageToken: queryResults.pageToken
    });
    rows = rows.concat(queryResults.rows);
  }
  
  // get or insert a sheet for the query
  var sheets = SpreadsheetApp.openByUrl(SPREADSHEET_URL);
  var sheet = getOrInsertSheetByTag(queryName, queryResults.schema.fields.length);
  
  // set the fields
  var fields = queryResults.schema.fields.map(function(field) {
    return field.name;
  });
  for (i = 0; i < fields.length; i++) {
    sheet.getRange(1, i + 1).setValue(fields[i]);
  }
  
  // read the results
  var rowSize = sheet.getMaxRows() - 1;
  var data = new Array();
  for (var i = 0; i < rowSize; i++) {
    if (rows[i]) {
      // if there's a row in the result, copy the columns
      var cols = rows[i].f;
      data[i] = new Array(cols.length);
      for (var j = 0; j < cols.length; j++) {
        data[i][j] = cols[j].v;
      }
    } else {
      // if there's no more rows in the result, set null to the columns
      data[i] = new Array(fields.length);
      for (var j = 0; j < fields.length; j++) {
        data[i][j] = null;
      }
    }
  }
  
  // set the results to the sheet
  sheet.getRange(2, 1, rowSize, fields.length).setValues(data);
  Logger.log("Updated " + rows.length + " results.");
}

// initialization
function onOpen() {

  // custom menu for BQ query manual execution
  var ui = SpreadsheetApp.getUi();
  ui.createMenu('Dashboard')
      .addItem('Run All BQ Queries', 'runQueriesManually')
      .addToUi();

  // run queries
  runQueries();
}

function runQueriesManually() {
  
  // get bqSheet
  var sheets = SpreadsheetApp.openByUrl(SPREADSHEET_URL);
  var bqSheet = sheets.getSheetByName(BQ_SHEET_NAME);
  
  // clear last execution times
  var rowIndex = 2;
  while(isQueryAvailableAt(bqSheet, rowIndex)) {
    bqSheet.getRange(rowIndex, BQ_COL_LASTTIME).clear();
    rowIndex += 1;
  }
  
  // run queries
  runQueries(true);
}


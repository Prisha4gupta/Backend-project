/**
 * Google Apps Script: Sheet to Database Integration
 * -------------------------------------------------
 * Automatically syncs student data from Google Sheets to the backend API.
 * 
 * Features:
 * - onEdit trigger for real-time sync
 * - Email validation
 * - Error highlighting
 * - Batch processing support
 * 
 * Setup:
 * 1. Open your Google Sheet
 * 2. Go to Extensions > Apps Script
 * 3. Paste this code
 * 4. Configure the API_ENDPOINT
 * 5. Run setupTrigger() once to enable auto-sync
 * 
 * Author: Data Engineering Team
 */

// ============================================
// CONFIGURATION
// ============================================

// Replace with your actual API endpoint
const API_ENDPOINT = 'https://your-api-endpoint.com/register-student';

// Expected column headers (0-indexed)
const COLUMNS = {
  STUDENT_CODE: 0,    // A
  FIRST_NAME: 1,      // B
  LAST_NAME: 2,       // C
  EMAIL: 3,           // D
  DATE_OF_BIRTH: 4,   // E
  GENDER: 5,          // F
  PHONE: 6,           // G
  DEPARTMENT_CODE: 7, // H
  GRADUATION_YEAR: 8, // I
  GPA: 9,             // J
  STATUS: 10          // K (for sync status)
};

// Colors for cell highlighting
const COLORS = {
  SUCCESS: '#d4edda',    // Green
  ERROR: '#f8d7da',      // Red
  WARNING: '#fff3cd',    // Yellow
  PENDING: '#cce5ff',    // Blue
  DEFAULT: '#ffffff'     // White
};

// Valid values
const VALID_GENDERS = ['Male', 'Female', 'Other', 'Prefer not to say'];
const VALID_DEPARTMENTS = ['CS', 'MATH', 'PHY', 'ENG', 'BIO', 'CHEM', 'ECON', 'PSY'];


// ============================================
// TRIGGERS
// ============================================

/**
 * Run this function once to set up the onEdit trigger.
 */
function setupTrigger() {
  // Remove existing triggers
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === 'onEditTrigger') {
      ScriptApp.deleteTrigger(trigger);
    }
  });
  
  // Create new trigger
  ScriptApp.newTrigger('onEditTrigger')
    .forSpreadsheet(SpreadsheetApp.getActive())
    .onEdit()
    .create();
  
  Logger.log('Trigger setup complete!');
  SpreadsheetApp.getUi().alert('Auto-sync trigger has been set up successfully!');
}


/**
 * Triggered when a cell is edited.
 * Validates and syncs the row to the database.
 */
function onEditTrigger(e) {
  const sheet = e.source.getActiveSheet();
  const range = e.range;
  const row = range.getRow();
  
  // Skip header row
  if (row === 1) return;
  
  // Skip if editing status column
  if (range.getColumn() === COLUMNS.STATUS + 1) return;
  
  // Get the edited row data
  const rowData = sheet.getRange(row, 1, 1, 11).getValues()[0];
  
  // Check if row has essential data
  if (!rowData[COLUMNS.STUDENT_CODE] || !rowData[COLUMNS.EMAIL]) {
    return; // Skip incomplete rows
  }
  
  // Validate and process
  processRow(sheet, row, rowData);
}


// ============================================
// VALIDATION FUNCTIONS
// ============================================

/**
 * Validates email format.
 * @param {string} email - Email to validate
 * @returns {boolean} - True if valid
 */
function isValidEmail(email) {
  if (!email) return false;
  const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
  return emailRegex.test(email.toString().trim());
}


/**
 * Validates student code format.
 * @param {string} code - Student code to validate
 * @returns {boolean} - True if valid
 */
function isValidStudentCode(code) {
  if (!code) return false;
  // Expecting format like STU001, STU123, etc.
  const codeRegex = /^[A-Z]{2,5}\d{2,5}$/;
  return codeRegex.test(code.toString().trim().toUpperCase());
}


/**
 * Validates GPA value.
 * @param {number|string} gpa - GPA to validate
 * @returns {boolean} - True if valid
 */
function isValidGPA(gpa) {
  if (gpa === '' || gpa === null || gpa === undefined) return true; // Optional
  const gpaNum = parseFloat(gpa);
  return !isNaN(gpaNum) && gpaNum >= 0 && gpaNum <= 4.0;
}


/**
 * Validates graduation year.
 * @param {number|string} year - Year to validate
 * @returns {boolean} - True if valid
 */
function isValidGraduationYear(year) {
  if (year === '' || year === null || year === undefined) return true; // Optional
  const yearNum = parseInt(year);
  const currentYear = new Date().getFullYear();
  return !isNaN(yearNum) && yearNum >= 2000 && yearNum <= currentYear + 10;
}


/**
 * Validates a complete row of student data.
 * @param {Array} rowData - Row data array
 * @returns {Object} - Validation result with isValid and errors
 */
function validateRow(rowData) {
  const errors = [];
  
  // Required fields
  if (!rowData[COLUMNS.STUDENT_CODE]) {
    errors.push('Student code is required');
  } else if (!isValidStudentCode(rowData[COLUMNS.STUDENT_CODE])) {
    errors.push('Invalid student code format (expected: ABC123)');
  }
  
  if (!rowData[COLUMNS.FIRST_NAME]) {
    errors.push('First name is required');
  }
  
  if (!rowData[COLUMNS.LAST_NAME]) {
    errors.push('Last name is required');
  }
  
  if (!rowData[COLUMNS.EMAIL]) {
    errors.push('Email is required');
  } else if (!isValidEmail(rowData[COLUMNS.EMAIL])) {
    errors.push('Invalid email format');
  }
  
  // Optional field validations
  if (rowData[COLUMNS.GENDER] && !VALID_GENDERS.includes(rowData[COLUMNS.GENDER])) {
    errors.push(`Invalid gender. Valid options: ${VALID_GENDERS.join(', ')}`);
  }
  
  if (rowData[COLUMNS.DEPARTMENT_CODE]) {
    const dept = rowData[COLUMNS.DEPARTMENT_CODE].toString().toUpperCase();
    if (!VALID_DEPARTMENTS.includes(dept)) {
      errors.push(`Invalid department. Valid options: ${VALID_DEPARTMENTS.join(', ')}`);
    }
  }
  
  if (!isValidGPA(rowData[COLUMNS.GPA])) {
    errors.push('Invalid GPA (must be 0.0 - 4.0)');
  }
  
  if (!isValidGraduationYear(rowData[COLUMNS.GRADUATION_YEAR])) {
    errors.push('Invalid graduation year');
  }
  
  return {
    isValid: errors.length === 0,
    errors: errors
  };
}


// ============================================
// PROCESSING FUNCTIONS
// ============================================

/**
 * Processes a single row - validates and syncs to API.
 * @param {Sheet} sheet - The spreadsheet sheet
 * @param {number} row - Row number
 * @param {Array} rowData - Row data array
 */
function processRow(sheet, row, rowData) {
  const statusCell = sheet.getRange(row, COLUMNS.STATUS + 1);
  
  // Mark as pending
  statusCell.setValue('Processing...');
  highlightRow(sheet, row, COLORS.PENDING);
  
  // Validate
  const validation = validateRow(rowData);
  
  if (!validation.isValid) {
    // Highlight errors
    statusCell.setValue('Error: ' + validation.errors.join('; '));
    highlightRow(sheet, row, COLORS.ERROR);
    return;
  }
  
  // Prepare data for API
  const studentData = {
    student_code: rowData[COLUMNS.STUDENT_CODE].toString().trim().toUpperCase(),
    first_name: rowData[COLUMNS.FIRST_NAME].toString().trim(),
    last_name: rowData[COLUMNS.LAST_NAME].toString().trim(),
    email: rowData[COLUMNS.EMAIL].toString().trim().toLowerCase(),
    date_of_birth: formatDate(rowData[COLUMNS.DATE_OF_BIRTH]),
    gender: rowData[COLUMNS.GENDER] || null,
    phone: rowData[COLUMNS.PHONE] ? rowData[COLUMNS.PHONE].toString() : null,
    department_code: rowData[COLUMNS.DEPARTMENT_CODE] ? 
      rowData[COLUMNS.DEPARTMENT_CODE].toString().toUpperCase() : null,
    graduation_year: rowData[COLUMNS.GRADUATION_YEAR] ? 
      parseInt(rowData[COLUMNS.GRADUATION_YEAR]) : null,
    gpa: rowData[COLUMNS.GPA] !== '' ? parseFloat(rowData[COLUMNS.GPA]) : null
  };
  
  // Send to API
  try {
    const result = sendToAPI(studentData);
    
    if (result.success) {
      statusCell.setValue('Synced: ' + new Date().toLocaleString());
      highlightRow(sheet, row, COLORS.SUCCESS);
    } else {
      statusCell.setValue('API Error: ' + result.message);
      highlightRow(sheet, row, COLORS.WARNING);
    }
  } catch (error) {
    statusCell.setValue('Error: ' + error.message);
    highlightRow(sheet, row, COLORS.ERROR);
  }
}


/**
 * Formats a date value for the API.
 * @param {Date|string} dateValue - Date to format
 * @returns {string|null} - Formatted date string (YYYY-MM-DD) or null
 */
function formatDate(dateValue) {
  if (!dateValue) return null;
  
  try {
    const date = new Date(dateValue);
    if (isNaN(date.getTime())) return null;
    
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    
    return `${year}-${month}-${day}`;
  } catch (e) {
    return null;
  }
}


/**
 * Highlights a row with a specified color.
 * @param {Sheet} sheet - The spreadsheet sheet
 * @param {number} row - Row number
 * @param {string} color - Hex color code
 */
function highlightRow(sheet, row, color) {
  const range = sheet.getRange(row, 1, 1, 10); // Columns A-J
  range.setBackground(color);
}


// ============================================
// API FUNCTIONS
// ============================================

/**
 * Sends student data to the registration API.
 * @param {Object} studentData - Student data object
 * @returns {Object} - API response
 */
function sendToAPI(studentData) {
  const options = {
    method: 'POST',
    contentType: 'application/json',
    payload: JSON.stringify(studentData),
    muteHttpExceptions: true
  };
  
  try {
    const response = UrlFetchApp.fetch(API_ENDPOINT, options);
    const responseCode = response.getResponseCode();
    const responseBody = JSON.parse(response.getContentText());
    
    if (responseCode === 201 || responseCode === 200) {
      return {
        success: true,
        message: responseBody.message || 'Success',
        student_id: responseBody.student_id
      };
    } else if (responseCode === 409) {
      // Conflict - student already exists
      return {
        success: true,
        message: 'Already registered'
      };
    } else {
      return {
        success: false,
        message: responseBody.detail || responseBody.message || `HTTP ${responseCode}`
      };
    }
  } catch (error) {
    Logger.log('API Error: ' + error.toString());
    return {
      success: false,
      message: error.toString()
    };
  }
}


// ============================================
// BATCH OPERATIONS
// ============================================

/**
 * Syncs all rows in the sheet (use from menu).
 */
function syncAllRows() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  const lastRow = sheet.getLastRow();
  
  if (lastRow < 2) {
    SpreadsheetApp.getUi().alert('No data to sync.');
    return;
  }
  
  const ui = SpreadsheetApp.getUi();
  const response = ui.alert(
    'Sync All Rows',
    `This will sync rows 2 to ${lastRow}. Continue?`,
    ui.ButtonSet.YES_NO
  );
  
  if (response !== ui.Button.YES) return;
  
  let synced = 0;
  let errors = 0;
  
  for (let row = 2; row <= lastRow; row++) {
    const rowData = sheet.getRange(row, 1, 1, 11).getValues()[0];
    
    // Skip empty rows
    if (!rowData[COLUMNS.STUDENT_CODE] || !rowData[COLUMNS.EMAIL]) continue;
    
    try {
      processRow(sheet, row, rowData);
      synced++;
    } catch (e) {
      errors++;
    }
    
    // Small delay to avoid rate limiting
    Utilities.sleep(100);
  }
  
  ui.alert(`Sync Complete`, `Processed: ${synced} rows\nErrors: ${errors}`, ui.ButtonSet.OK);
}


/**
 * Validates all rows without syncing (use from menu).
 */
function validateAllRows() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  const lastRow = sheet.getLastRow();
  
  if (lastRow < 2) {
    SpreadsheetApp.getUi().alert('No data to validate.');
    return;
  }
  
  let valid = 0;
  let invalid = 0;
  
  for (let row = 2; row <= lastRow; row++) {
    const rowData = sheet.getRange(row, 1, 1, 11).getValues()[0];
    
    // Skip empty rows
    if (!rowData[COLUMNS.STUDENT_CODE] && !rowData[COLUMNS.EMAIL]) {
      highlightRow(sheet, row, COLORS.DEFAULT);
      continue;
    }
    
    const validation = validateRow(rowData);
    const statusCell = sheet.getRange(row, COLUMNS.STATUS + 1);
    
    if (validation.isValid) {
      statusCell.setValue('Valid');
      highlightRow(sheet, row, COLORS.SUCCESS);
      valid++;
    } else {
      statusCell.setValue('Invalid: ' + validation.errors.join('; '));
      highlightRow(sheet, row, COLORS.ERROR);
      invalid++;
    }
  }
  
  SpreadsheetApp.getUi().alert(
    'Validation Complete',
    `Valid: ${valid} rows\nInvalid: ${invalid} rows`,
    SpreadsheetApp.getUi().ButtonSet.OK
  );
}


/**
 * Clears all status and highlighting.
 */
function clearStatus() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  const lastRow = sheet.getLastRow();
  
  if (lastRow < 2) return;
  
  // Clear status column
  const statusRange = sheet.getRange(2, COLUMNS.STATUS + 1, lastRow - 1, 1);
  statusRange.clearContent();
  
  // Clear highlighting
  const dataRange = sheet.getRange(2, 1, lastRow - 1, 10);
  dataRange.setBackground(COLORS.DEFAULT);
  
  SpreadsheetApp.getUi().alert('Status cleared!');
}


// ============================================
// MENU
// ============================================

/**
 * Creates custom menu when spreadsheet opens.
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  
  ui.createMenu('🎓 Student Sync')
    .addItem('Setup Auto-Sync Trigger', 'setupTrigger')
    .addSeparator()
    .addItem('Validate All Rows', 'validateAllRows')
    .addItem('Sync All Rows', 'syncAllRows')
    .addSeparator()
    .addItem('Clear Status', 'clearStatus')
    .addToUi();
}

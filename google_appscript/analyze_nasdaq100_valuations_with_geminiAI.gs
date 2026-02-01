function analyzeStockWithGeminiAI() {
  // =========================================
  // Configuration: API keys and folder settings
  // =========================================
  var GEMINI_API_KEY = PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY'); // Gemini API key from Google AI Studio
  var FOLDER_ID = PropertiesService.getScriptProperties().getProperty('FOLDER_ID'); // Target Drive folder ID
  var RECIPIENT_EMAIL = PropertiesService.getScriptProperties().getProperty('RECIPIENT_EMAIL'); // Recipient email address
  // =========================================

  // 1) Locate the latest valuation file in the folder
  var folder = DriveApp.getFolderById(FOLDER_ID);
  var files = folder.getFiles();
  var latestFile = null;
  var latestTimestamp = 0;
  var namePattern = /nasdaq100_valuations_(\d{4}-\d{2}-\d{2})/;

  Logger.log("AI job started: searching for the latest file");

  while (files.hasNext()) {
    var file = files.next();
    var match = file.getName().match(namePattern);
    if (match) {
      var ts = new Date(match[1]).getTime();
      if (ts > latestTimestamp) {
        latestTimestamp = ts;
        latestFile = file;
      }
    }
  }

  if (!latestFile) {
    Logger.log("No valuation file found");
    return;
  }
  Logger.log("Found file: " + latestFile.getName());

  // 2) Read data and prepare it for the model
  // Keep only relevant columns to reduce token usage and improve readability
  var csvContent = "";
  if (latestFile.getMimeType() === MimeType.GOOGLE_SHEETS) {
    csvContent = convertSheetToCsv(SpreadsheetApp.open(latestFile).getSheets()[0]);
  } else {
    csvContent = latestFile.getBlob().getDataAsString();
  }
  
  // Optional: trim rows if the dataset is extremely large.
  // For ~100-200 tickers, sending the full file is acceptable.

  // 3) Call the Gemini API
  var analysisResult = callGeminiAPI(GEMINI_API_KEY, csvContent);
  
  if (!analysisResult) {
    Logger.log("No response from Gemini or an error occurred");
    return;
  }

  // 4) Send the email report
  var dateStr = Utilities.formatDate(new Date(latestTimestamp), Session.getScriptTimeZone(), "dd/MM/yyyy");
  var htmlBody = "<h2>🤖 บทวิเคราะห์หุ้น Nasdaq100 (AI Analyst)</h2>";
  htmlBody += "<p><b>ข้อมูลประจำวันที่:</b> " + dateStr + " (จากไฟล์ " + latestFile.getName() + ")</p>";
  htmlBody += "<hr>";
  
  // Convert the model's Markdown to HTML for email rendering
  var formattedAnalysis = formatMarkdownToHtml(analysisResult);
  htmlBody += formattedAnalysis;

  MailApp.sendEmail({
    to: RECIPIENT_EMAIL,
    subject: "📈 AI Market Insight: " + dateStr,
    htmlBody: htmlBody
  });

  Logger.log("Report email sent successfully");
}
// -----------------------------------------------------------
// Gemini API helper
// -----------------------------------------------------------
function callGeminiAPI(apiKey, dataContext) {
  var url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=" + apiKey;
  
  // Prompt instructions for the model
  var prompt = `
    คุณคือนักวิเคราะห์การลงทุนระดับโลก (Expert Financial Analyst)
    ฉันมีข้อมูลดิบ Valuation ของหุ้น Nasdaq 100 ตาม CSV ด้านล่างนี้
    
    ข้อมูลประกอบด้วย: Ticker, Price, PEG Ratio, Margin of Safety, Valuation Status
    
    หน้าที่ของคุณคือเขียน "สรุปภาวะตลาดและการลงทุนประจำวัน" เป็นภาษาไทย โดยต้องมีหัวข้อดังนี้:
    
    1. 🌍 **ภาพรวมตลาด (Market Sentiment):** ดูจากข้อมูลภาพรวมว่าหุ้นส่วนใหญ่ Overvalued หรือ Undervalued
    2. 💎 **หุ้น Value น่าสะสม (The Hidden Gems):** เลือก Top 3 ที่ Margin of Safety ดีที่สุด (หรือติดลบน้อยที่สุด ถ้าแดงทั้งกระดาน) พร้อมวิเคราะห์สั้นๆ ว่าทำไม
    3. 🚀 **หุ้น Growth ราคาเหมาะสม:** เลือก Top 3 ที่ PEG Ratio ต่ำกว่า 1 หรือใกล้เคียง 1 ที่สุด
    4. ⚠️ **หุ้นที่ต้องระวัง:** หุ้นที่ราคาแพงเกินไปมากๆ (Overvalued สูงๆ)
    5. 💡 **สรุปคำแนะนำ:** ควรทำอย่างไรในวันนี้ (ซื้อ, ถือ, หรือ ชะลอการลงทุน)

    **สำคัญ:** - ไม่ต้องแสดงตารางข้อมูลดิบทั้งหมด
    - ใช้ภาษาที่เป็นมืออาชีพแต่อ่านเข้าใจง่าย
    - จัดรูปแบบให้อ่านง่าย (ใช้ Bullet point, ตัวหนา)
    
    นี่คือข้อมูล CSV:
    ${dataContext}
  `;

  var payload = {
    "contents": [{
      "parts": [{ "text": prompt }]
    }]
  };

  var options = {
    "method": "post",
    "contentType": "application/json",
    "payload": JSON.stringify(payload),
    "muteHttpExceptions": true
  };

  try {
    var response = UrlFetchApp.fetch(url, options);
    var json = JSON.parse(response.getContentText());
    
    if (json.candidates && json.candidates.length > 0) {
      return json.candidates[0].content.parts[0].text;
    } else {
      Logger.log("Error from AI: " + JSON.stringify(json));
      return null;
    }
  } catch (e) {
    Logger.log("Exception calling Gemini: " + e.toString());
    return null;
  }
}

// -----------------------------------------------------------
// Helper utilities
// -----------------------------------------------------------

// Convert simple Markdown to HTML for email output
function formatMarkdownToHtml(text) {
  var html = text
    .replace(/\*\*(.*?)\*\*/g, '<b>$1</b>') // Bold
    .replace(/\n/g, '<br>') // New line
    .replace(/## (.*?)(<br>|$)/g, '<h3 style="color:#2c3e50;">$1</h3>') // Heading
    .replace(/- /g, '• '); // Bullet
  
  return "<div style='font-family: Sarabun, sans-serif; font-size: 16px; line-height: 1.6; color: #333;'>" + html + "</div>";
}

// Convert a Google Sheet to CSV when the source is a Sheet
function convertSheetToCsv(sheet) {
  var data = sheet.getDataRange().getValues();
  var csv = "";
  for (var i = 0; i < data.length; i++) {
    csv += data[i].join(",") + "\n";
  }
  return csv;
}

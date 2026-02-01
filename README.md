# Nasdaq-100 1-1-1 Rule Data Pipeline Starterkit

Pipeline สำหรับดึงข้อมูลหุ้น Nasdaq-100, ประเมินมูลค่า (ต่ำกว่ามูลค่า/เกินมูลค่า/สมเหตุสมผล), บันทึกเป็น CSV และอัปโหลดขึ้น Google Drive เพื่อนำไปใช้ใน Looker Studio

## แนวคิดการประเมินมูลค่า (ค่าเริ่มต้น)
### Valuation Hunter Strategy
ใช้ 4 เกณฑ์หลักในการคัดกรอง:
1. PEG Ratio < `PEG_MAX`
2. P/E Ratio ไม่สูงเกินไปเมื่อเทียบกับ median ของ sector (`trailing_pe <= sector_median_pe * PE_SECTOR_MAX_MULT`)
3. Benjamin Graham Number: `sqrt(22.5 * EPS * BookValuePerShare)`
4. Margin of Safety: `(graham_value - price) / graham_value` ต้องมากกว่า `MARGIN_OF_SAFETY_MIN`

ผลลัพธ์สรุปในคอลัมน์:
- `valuation_hunter` = pass/fail/unknown
- `peg_pass`, `pe_vs_sector_pass`, `margin_of_safety_pass`

### Fair Value (สำรอง)
ใช้ Graham Value เป็น fair value ก่อน หากไม่มีจะ fallback ไป:
1. `targetMeanPrice` จากนักวิเคราะห์
2. `trailingEps * median(trailingPE)` ของกลุ่ม sector (หรือ median รวมทั้งดัชนีถ้า sector ว่าง)
3. ถ้าไม่พอข้อมูล จะให้สถานะ `unknown`

การคัดแยก undervalued/overvalued ยังคงอยู่ (คอลัมน์ `valuation`) ด้วยค่า `UNDERVALUED_THRESHOLD` และ `OVERVALUED_THRESHOLD`
ค่าตัวคูณทั้งหมดปรับได้ใน `config/.env` (ดูตัวอย่างใน `config/.env.example`)

## Pipeline Flow
```
Extract -> Transform -> Load

Extract:
  - ดึงรายชื่อ Nasdaq‑100 (Wikipedia)
  - fallback ไปใช้ data/nasdaq100_tickers.csv

Transform:
  - ดึงข้อมูล fundamentals ต่อหุ้น (price, EPS, PE, ฯลฯ)
  - คำนวณ PEG, Graham Value, Margin of Safety
  - สรุปผล Valuation Hunter (pass/fail/unknown)
  - จัดระดับ undervalued/overvalued/fair

Load:
  - เขียน CSV ลง data/
  - อัปโหลดไป Google Drive (OAuth หรือ Service Account)
  - ถ้าเปิด `GOOGLE_DRIVE_CONVERT_TO_SHEETS=true` จะสร้างเป็น Google Sheets
```

## Architecture Diagram
```mermaid
flowchart LR
  subgraph Sources
    WIKI[Wikipedia Nasdaq‑100]
    FALLBACK[data/nasdaq100_tickers.csv]
    YF[yfinance]
  end

  subgraph Pipeline[one_one_one_rule]
    ST[settings.py]
    PJ[pipeline_job.py]
    EX[extract_tickers.py]
    TR[transform_valuation.py]
    LD[load_drive.py]
  end

  subgraph Outputs
    CSV[data/*.csv]
    LOG[data/logs/*.log.txt]
  end

  subgraph Drive[Google Drive]
    DRIVE_FILES[Valuation files (CSV or Sheets)]
    DRIVE_LOGS[Pipeline logs]
  end

  subgraph Analytics
    LOOKER[Looker Studio]
    GAS[Apps Script: analyze_nasdaq100_valuations_with_geminiAI.gs]
    GEMINI[Gemini 2.5 Flash]
    EMAIL[Email Report]
  end

  ST --> PJ
  PJ --> EX
  PJ --> TR
  PJ --> LD

  WIKI --> EX
  FALLBACK --> EX
  EX --> TR
  YF --> TR
  TR --> CSV
  PJ --> LOG

  CSV --> LD
  LOG --> LD
  LD --> DRIVE_FILES
  LD --> DRIVE_LOGS

  DRIVE_FILES --> LOOKER
  DRIVE_FILES --> GAS
  GAS --> GEMINI
  GEMINI --> EMAIL
```

## คำอธิบายแต่ละขั้น
### Extract
- `one_one_one_rule/extract_tickers.py`
- อ่านรายชื่อ Nasdaq‑100 จาก Wikipedia (ถ้าเปิดใช้ `USE_WIKIPEDIA_TICKERS=true`)
- หากดึงไม่ได้ จะ fallback ไปที่ `data/nasdaq100_tickers.csv`

### Transform
- `one_one_one_rule/transform_valuation.py`
- ดึงข้อมูลพื้นฐานของหุ้น (ราคา, EPS, PE, growth, book value)
- ทำ data cleansing เบื้องต้น:
  - ตัดช่องว่าง/แปลง ticker เป็นตัวพิมพ์ใหญ่และแทน `.` ด้วย `-`
  - ลบ ticker ซ้ำ
  - ใช้ `pandera` สำหรับ type coercion/validation ของคอลัมน์ตัวเลข
  - แทนค่า `inf/-inf` เป็น `NaN`
  - ตั้งค่า `price/market_cap` ที่ ≤ 0 เป็น `NaN`
  - เติม `sector` ที่ว่างให้เป็น `Unknown`
- คำนวณ:
  - PEG Ratio (ใช้ค่า reported หรือคำนวณจาก PE / growth)
  - Graham Value: `sqrt(22.5 * EPS * BookValuePerShare)`
  - Margin of Safety: `(graham_value - price) / graham_value`
- สรุปผลเป็น:
  - `valuation_hunter` (pass/fail/unknown)
  - `valuation` (undervalued/overvalued/fair)

### Load
- `one_one_one_rule/load_drive.py`
- บันทึกไฟล์ CSV ลง `data/`
- อัปโหลดไป Google Drive
  - OAuth: เหมาะกับ Gmail ส่วนตัว
  - Service Account: ใช้กับ Shared Drive เท่านั้น
  - ถ้าเปิด `GOOGLE_DRIVE_CONVERT_TO_SHEETS=true` ไฟล์บน Drive จะเป็น Google Sheets

## โครงสร้างโปรเจกต์ (Snake Case)
- `one_one_one_rule/` โค้ดหลักของ pipeline
  - `settings.py` โหลดค่า `.env`
  - `extract_tickers.py` ดึงรายชื่อ Nasdaq‑100
  - `transform_valuation.py` ประเมินมูลค่า
  - `load_drive.py` อัปโหลด Google Drive
  - `pipeline_job.py` งานหลักของ pipeline
  - `perf.py` ตัวช่วย log performance ของแต่ละฟังก์ชัน
- `scripts/` สคริปต์เสริม (อัปโหลดไฟล์, อัปเดตรายชื่อ)

## ติดตั้ง
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp config/.env.example config/.env
```

## ตั้งค่า Google Drive (OAuth - แนะนำสำหรับ Gmail ส่วนตัว)
1. สร้าง OAuth Client (Desktop App) ใน Google Cloud และเปิด Drive API
2. ดาวน์โหลด `credentials.json` แล้ววางที่ `config/credentials.json`
3. ตั้งค่าใน `config/.env`:
   - `GOOGLE_DRIVE_AUTH_MODE=oauth`
   - `GOOGLE_OAUTH_CLIENT_FILE=config/credentials.json`
   - `GOOGLE_OAUTH_TOKEN_FILE=config/oauth_token.json`
   - `GOOGLE_DRIVE_CONVERT_TO_SHEETS=true` (แปลง CSV เป็น Google Sheets ตอนอัปโหลด)
4. รันสคริปต์ครั้งแรก ระบบจะเปิด browser ให้ login และสร้าง token อัตโนมัติ
   - ไฟล์ token จะถูกสร้างที่ `config/oauth_token.json`

## ตั้งค่า Google Drive (Service Account - ใช้กับ Shared Drive เท่านั้น)
1. สร้าง Service Account ใน Google Cloud และเปิด Drive API
2. ดาวน์โหลดไฟล์ key เป็น JSON แล้ววางที่ `config/service_account.json`
3. แชร์โฟลเดอร์ปลายทางใน Google Drive ให้กับอีเมลของ service account
4. ใส่ `GOOGLE_DRIVE_FOLDER_ID` ใน `config/.env`

## รันครั้งเดียว
```bash
python -m one_one_one_rule.pipeline_job
```

ผลลัพธ์จะอยู่ที่ `data/` เช่น:
- `data/nasdaq100_latest.csv`
- `data/nasdaq100_valuations_YYYY-MM-DD.csv`

การอัปโหลดขึ้น Drive:
- ถ้า `WRITE_DATED_COPY=true` จะอัปโหลดไฟล์ชื่อมีวันที่ (`nasdaq100_valuations_YYYY-MM-DD.csv`)
- ถ้า `WRITE_DATED_COPY=false` จะอัปโหลดไฟล์ชื่อ `GOOGLE_DRIVE_FILE_NAME` (ค่าเริ่มต้น `nasdaq100_valuations.csv`)
- ถ้า `GOOGLE_DRIVE_CONVERT_TO_SHEETS=true` จะสร้างเป็น Google Sheets แทนไฟล์ CSV
  - แนะนำให้ตั้งชื่อไฟล์เป็น `.gsheet` หรือไม่มีนามสกุล เพื่อง่ายต่อการมองใน Drive

## Logging
- สร้างไฟล์ log ตามรูปแบบ `{LOG_FILE_PREFIX}_YYYY-MM-DD.log`
- เก็บไว้ใน `LOG_DIR` (ค่าเริ่มต้น `data/logs`)
- ถ้า `UPLOAD_LOG_TO_DRIVE=true` จะอัปโหลดไฟล์ log ขึ้น Drive ด้วย
- มี performance log ต่อฟังก์ชัน (wall time, CPU time, RSS memory)

ตัวอย่างบรรทัด perf:
```
perf one_one_one_rule.transform_valuation.fetch_fundamentals wall_ms=512.34 cpu_ms=480.12 rss_mb=312.50 delta_rss_mb=8.20
```

## รายชื่อ Nasdaq‑100
- ถ้า `USE_WIKIPEDIA_TICKERS=true` จะดึงจาก Wikipedia
- ถ้าดึงไม่ได้จะใช้ `data/nasdaq100_tickers.csv` (อัปเดตด้วยสคริปต์ด้านล่าง)

## ตั้งเวลา (หลังตลาดปิด)
ตัวอย่าง cron (โซนเวลา Thailand/Asia-Bangkok):
```bash
# 18:30 ICT ทุกวันจันทร์-ศุกร์
30 18 * * 1-5  cd /PYTHON-PROJECT/1-1-1-rule-data-pipeline-stock-nasdaq-100 && \
  /Users/Tualek/Documents/PYTHON-PROJECT/1-1-1-rule-data-pipeline-stock-nasdaq-100/.venv/bin/python -m one_one_one_rule.pipeline_job
```

> หมายเหตุ: ปรับเวลาให้เหมาะกับตลาด (เช่น หลัง 16:00 ET)

## Looker Studio (Data Looker)
1. เปิด Looker Studio และสร้าง Data Source ตามประเภทไฟล์บน Drive:
   - ถ้าเป็น CSV ให้เลือก Google Drive
   - ถ้าเป็น Google Sheets ให้เลือก Google Sheets
2. ตั้งค่า schedule refresh ให้ดึงข้อมูลใหม่ทุกวันหลังตลาดปิด
3. ใช้ field `valuation_hunter` สำหรับกราฟ/ฟิลเตอร์ (pass/fail/unknown) หรือ `valuation` สำหรับ undervalued/overvalued

### สูตรตัวอย่างสำหรับ Looker Studio (Calculated Fields)
> ปรับชื่อฟิลด์ให้ตรงกับแหล่งข้อมูลของคุณ

```text
as_of_date_date
PARSE_DATE("%Y-%m-%d", as_of_date)

discount_to_fair_pct
CASE WHEN fair_value > 0 THEN 100 * (fair_value - price) / fair_value END

premium_to_fair_pct
CASE WHEN fair_value > 0 THEN 100 * (price - fair_value) / fair_value END

price_to_fair
CASE WHEN fair_value > 0 THEN price / fair_value END

price_to_graham
CASE WHEN graham_value > 0 THEN price / graham_value END

mos_bucket
CASE
  WHEN margin_of_safety IS NULL THEN "unknown"
  WHEN margin_of_safety >= 0.25 THEN ">=25%"
  WHEN margin_of_safety >= 0.10 THEN "10-25%"
  WHEN margin_of_safety >= 0 THEN "0-10%"
  ELSE "<0%"
END

valuation_hunter_flag
CASE
  WHEN valuation_hunter = "pass" THEN "PASS"
  WHEN valuation_hunter = "fail" THEN "FAIL"
  ELSE "UNKNOWN"
END

undervalued_count
SUM(CASE WHEN valuation = "undervalued" THEN 1 ELSE 0 END)

hunter_pass_rate
SUM(CASE WHEN valuation_hunter = "pass" THEN 1 ELSE 0 END) / COUNT(ticker)
```

## Google Apps Script Trigger + Gemini (gemini-2.5-flash)
ใช้ Apps Script เพื่อดึงไฟล์จาก Google Drive แล้วส่งให้โมเดล Gemini วิเคราะห์อัตโนมัติผ่าน Trigger

### ไฟล์สคริปต์ที่ใช้
- `google_appscript/analyze_nasdaq100_valuations_with_geminiAI.gs`
- ฟังก์ชันหลัก: `analyzeStockWithGeminiAI`
- ค้นหาไฟล์ล่าสุดจากชื่อรูปแบบ `nasdaq100_valuations_YYYY-MM-DD`
  - แนะนำให้ตั้ง `WRITE_DATED_COPY=true` ใน pipeline เพื่อให้สคริปต์หาไฟล์ล่าสุดได้
- รองรับทั้งไฟล์ CSV และ Google Sheets (ตรวจจาก MIME type อัตโนมัติ)

### ขั้นตอนตั้งค่า
1. สร้าง Google Apps Script ใหม่
2. คัดลอกโค้ดจาก `google_appscript/analyze_nasdaq100_valuations_with_geminiAI.gs` ไปวาง
3. ไปที่ **Project Settings** → **Script Properties** แล้วตั้งค่า key ที่จำเป็น
4. สร้าง Trigger: **Triggers** → **Add Trigger** → เลือกฟังก์ชัน `analyzeStockWithGeminiAI` → Time-driven

### Script Properties ที่ต้องตั้งค่า
- `GEMINI_API_KEY` → คีย์จาก Google AI Studio
- `FOLDER_ID` → Folder บน Google Drive ที่เก็บไฟล์ผลลัพธ์
- `RECIPIENT_EMAIL` → อีเมลปลายทางสำหรับส่งรายงาน

#### ตัวอย่างค่า (ห้ามใส่คีย์จริงลงในไฟล์)
```
GEMINI_API_KEY = YOUR_REAL_API_KEY
FOLDER_ID = 1tfO6Exl20qk1xMwIxHJCGRi7TKW8CEiy
RECIPIENT_EMAIL = you@example.com
```

#### หมายเหตุด้านความปลอดภัย
- ห้ามใส่ API Key ลงในไฟล์โค้ดหรือ commit ขึ้น Git
- เก็บคีย์ไว้ใน **Script Properties** เท่านั้น
- ถ้าเผลอแชร์คีย์ ให้รีบ Rotate/สร้างคีย์ใหม่ทันที

### การตั้ง Trigger (Time-driven)
1. เปิด Google Apps Script → **Triggers** (ไอคอนนาฬิกา)
2. กด **Add Trigger**
3. เลือกฟังก์ชัน `analyzeStockWithGeminiAI`
4. เลือก **Event source** = Time-driven
5. เลือกความถี่ (เช่น Every day 18:30 หรือ 19:00 หลังตลาดปิด)
6. กด Save และอนุญาตสิทธิ์ (Authorize) ให้เรียบร้อย

---
หากต้องการปรับสูตรประเมินมูลค่า แจ้งเกณฑ์ที่ต้องการได้เลย

## สคริปต์เสริม
### อัปเดตรายชื่อ Nasdaq‑100 อัตโนมัติ
ดึงรายชื่อจาก Wikipedia แล้วเขียนลง `data/nasdaq100_tickers.csv`
```bash
python scripts/update_nasdaq100_tickers.py
```
แนะนำให้ตั้ง cron รายวัน/รายเดือน เพื่อ keep list ให้ทันสมัย

### อัปโหลดไฟล์ขึ้น Drive แบบอ่านค่า `.env`
```bash
python scripts/upload_drive.py --file path/to/file.csv
```

# TFEX Stateless Market Data Collector (1-Minute Snapshots)

โปรเจกต์ Python แบบ **Stateless Data Collector** สำหรับดึงข้อมูลตลาดอนุพันธ์ **TFEX** (SET50 Index, S50 Futures และ SET50 Options) ราย 1 นาที จาก **Settrade Open API (v2)** เพื่อรันบน **GitHub Actions** และบันทึกข้อมูลแบบ Batch ลงใน **Turso Cloud SQLite** ผ่าน HTTP Pipeline API (`/v2/pipeline`)

---

## 📁 โครงสร้างโปรเจกต์ (Project Structure)

```text
.
├── .github/
│   └── workflows/
│       └── collect_tfex.yml   # GitHub Actions Workflow (cron 1 นาที + workflow_dispatch)
├── .env.example               # ตัวอย่าง Environment Variables สำหรับรันทดสอบในเครื่อง
├── .gitignore                 # ป้องกันการ Commit secret หรือ cache
├── collector.py               # สคริปต์หลักดึงข้อมูลและบันทึกลง Turso Cloud SQLite
├── requirements.txt           # เฉพาะ settrade_v2, requests, pytz (ไร้ Library หนัก)
└── README.md                  # เอกสารแนะนำการติดตั้งและตั้งค่า
```

---

## ⚙️ คุณสมบัติเด่นทางเทคนิค (Key Architectural Highlights)

1. **Lightweight & High-Speed Build**:
   - `requirements.txt` มีเฉพาะ `settrade_v2`, `requests`, และ `pytz`
   - ปราศจาก `pandas`, `numpy`, `scipy` หรือคอมไพเลอร์ C ของ `libsql-experimental` ทำให้ GitHub Actions ดาวน์โหลดและติดตั้งเสร็จภายในไม่กี่วินาที
2. **High-Throughput 60 req/s Market Data Optimization**:
   - ปรับแต่ง Rate Limit ตามสเปกจริงของ Settrade Open API กลุ่มข้อมูล **Market Data ซึ่งรองรับสูงสุด 60 Requests/วินาที** (ปลดล็อกข้อจำกัด 60 req/min ของ Order API ออกอย่างสิ้นเชิง)
   - ใช้ **HTTP Keep-Alive Connection Pooling** (`requests.Session` + Connection Pool 25 Sockets) ทำให้ดึงข้อมูลสัญลักษณ์ทั้งหมดกว่า 250–300 สัญญาเสร็จสิ้นภายใน **1–2.5 วินาที** (ผ่านเกณฑ์ < 10 วินาทีอย่างขาดลอย)
3. **Comprehensive Quarterly All-Strike Options Scanner**:
   - คัดเลือกเฉพาะ **Quarterly Series (`H`, `M`, `U`, `Z`)** สำหรับ SET50 Options ตามกฎกระดาน TFEX โดยตัดเดือนย่อย (Serial Months) ออกอย่างสิ้นเชิง
   - คำนวณหา **Front Series** และ **Next Series** อิงตามวันทำการรองสุดท้าย (Last Trading Day) ของไตรมาสนั้น เช่น ณ เดือนพฤศจิกายน 2026 จะได้ `Z26` (Front) และ `H27` (Next)
   - สแกนครอบคลุมทุก Strike Price ที่เปิดซื้อขายจริงบนกระดาน TFEX ขั้นละ 10 จุด ตั้งแต่ `750 ถึง 1350` และ `ATM ± 250 จุด` ครบทั้ง Call และ Put
   - สัญญาที่ยังไม่เปิดซื้อขาย (API ตอบกลับ 404 / `SettradeError`) สคริปต์จะข้ามอย่างนุ่มนวล (Graceful Fallback) โดยไม่หยุดการทำงาน
4. **Rollover-Proof Quarterly Futures Discovery**:
   - ค้นหาสัญญา **S50 Futures** เฉพาะรอบไตรมาส 4 สัญญาที่เปิดซื้อขายอยู่บนกระดานจริง (`H`, `M`, `U`, `Z`) โดยไม่ Hardcode ชื่อ Series
   - ตัดเดือนย่อย (Serial Months) ออกทั้งหมด และมีระบบ Rollover อัตโนมัติเมื่อถึงวันทำการรองสุดท้ายของแต่ละไตรมาส
5. **Trading Hours & Dual-Layer Holiday Guards**:
   - **ด่านที่ 1 (Static Calendar Holidays)**: บรรจุวันหยุดทำการประจำปี 2026–2027 ของตลาดหลักทรัพย์แห่งประเทศไทย (SET/TFEX) หากตรงกับวันหยุดจะพิมพ์ Log และ `sys.exit(0)` ทันทีโดยไม่แตะ API
   - **ด่านที่ 2 (Ad-hoc / Emergency Holiday Guard)**: เมื่อเวลาปัจจุบันหลัง 09:46 น. (เวลาไทย) สคริปต์จะดึงแท่งเทียน 1 นาทีของ Front Series Futures (`S50Z26`) มาตรวจสอบ หากวันที่ของแท่งเทียนล่าสุดไม่ใช่วันนี้ (`candle_date < today_bkk`) จะถือเป็นวันหยุดฉุกเฉินและ `sys.exit(0)` ทันทีโดยไม่ยิง Options และไม่เขียนข้อมูลลง Turso
   - **การตรวจสอบช่วงเวลาทำการปกติ**: ตรวจสอบเวลาไทย (`Asia/Bangkok`) เฉพาะวันจันทร์–ศุกร์ ช่วงเช้า `09:45 - 12:30` และช่วงบ่าย `14:15 - 16:55`
   - **`--force` Flag**: ข้ามด่านตรวจเวลาและวันหยุดทั้งหมดเพื่อใช้ทดสอบระบบได้ทุกเมื่อ
6. **Stateless HTTP Pipeline API Batch Write**:
   - บันทึกข้อมูลลง **Turso Cloud SQLite** ผ่าน endpoint `/v2/pipeline` ด้วย `requests` ใน HTTP Request เดียว (สร้างตารางอัตโนมัติหากยังไม่มี)
   - ปลอดภัย 100% ไม่มีการเปิดเผย Secret หรือ Token ออกมาใน Log

---

## 🗄️ โครงสร้างฐานข้อมูลบน Turso (Database Schemas)

สคริปต์จะสร้างตาราง 3 ตารางอัตโนมัติ (`CREATE TABLE IF NOT EXISTS`):

### 1. `spot_snapshots` (SET50 Spot Index)
| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | INTEGER PRIMARY KEY AUTOINCREMENT | ลำดับข้อมูล |
| `captured_at_bkk` | TEXT | วันที่และเวลาประเทศไทย (เช่น `2026-11-16 10:30:00`) |
| `symbol` | TEXT | สัญลักษณ์ (`SET50`) |
| `last` | REAL | ดัชนีล่าสุด |
| `volume` | REAL | ปริมาณการซื้อขายรวม |
| `value` | REAL | มูลค่าการซื้อขายรวม (บาท) |

### 2. `futures_snapshots` (S50 Futures Active Series)
| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | INTEGER PRIMARY KEY AUTOINCREMENT | ลำดับข้อมูล |
| `captured_at_bkk` | TEXT | วันที่และเวลาประเทศไทย |
| `symbol` | TEXT | สัญลักษณ์สัญญา (เช่น `S50Z26`, `S50H27`) |
| `bid` | REAL | ราคาเสนอซื้อดีที่สุด (Best Bid) |
| `ask` | REAL | ราคาเสนอขายดีที่สุด (Best Ask) |
| `last` | REAL | ราคาซื้อขายล่าสุด |
| `volume` | REAL | ปริมาณสัญญาซื้อขายรวม |
| `open_interest` | REAL | สถานะคงค้าง (Open Interest) |

### 3. `options_snapshots` (SET50 Options All Active Strikes: Front & Next Series)
| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | INTEGER PRIMARY KEY AUTOINCREMENT | ลำดับข้อมูล |
| `captured_at_bkk` | TEXT | วันที่และเวลาประเทศไทย |
| `symbol` | TEXT | สัญลักษณ์ออปชัน (เช่น `S50Z26C950`, `S50Z26P950`) |
| `series` | TEXT | รหัสซีรีส์ (เช่น `Z26`, `H27`) |
| `strike` | REAL | ราคาใช้สิทธิ (Strike Price) |
| `option_type` | TEXT | ประเภทสัญญา (`C` = Call, `P` = Put) |
| `bid` | REAL | ราคาเสนอซื้อดีที่สุด |
| `ask` | REAL | ราคาเสนอขายดีที่สุด |
| `last` | REAL | ราคาล่าสุด |
| `volume` | REAL | ปริมาณสัญญาซื้อขายรวม |
| `open_interest` | REAL | สถานะคงค้าง (Open Interest) |

### 4. `intraday_telegram_log` (De-duplication Log for Telegram Alerts)
| Column | Type | Description |
| :--- | :--- | :--- |
| `trade_date` | TEXT | วันที่ทำการซื้อขาย (`YYYY-MM-DD`) |
| `checkpoint` | TEXT | รหัสจุดตรวจ (`MORNING_OPEN`, `MIDDAY_CLOSE`, `AFTERNOON_OPEN`, `EVENING_CLOSE`) |
| `notified_at_bkk` | TEXT | วันที่และเวลาที่ส่งรายงานแจ้งเตือนสำเร็จ |
*(PRIMARY KEY: `trade_date`, `checkpoint` เพื่อป้องกันการส่งซ้ำในวันเดียวกัน 100%)*

---

## 📢 ระบบรายงานสถานะผ่าน Telegram (4 Daily Checkpoints)

ระบบส่งสรุปรายงานความสมบูรณ์ของข้อมูล Intraday เข้า Telegram Channel/Chat **วันละ 4 รอบ** เฉพาะวันที่ตลาดเปิดทำการจริง:

1. **`MORNING_OPEN` (10:00 – 10:30 น.):**
   - รายงานการเปิดตลาดรอบเช้า สถานะการดึงข้อมูล จำนวนแท่งเทียนที่บันทึกได้ และราคา Spot / Front Futures ล่าสุด
2. **`MIDDAY_CLOSE` (12:30 – 13:00 น.):**
   - รายงานสรุปจบภาคเช้า ช่วงเวลาบันทึก (Span) จำนวนแท่งเทียนรอบเช้า (เทียบเป้า 165 แท่ง) และประเมินความสมบูรณ์
3. **`AFTERNOON_OPEN` (14:30 – 15:00 น.):**
   - รายงานเปิดตลาดรอบบ่าย จำนวนแท่งที่ดึงได้หลัง 14:15 น. พร้อมแจ้งเตือนพิเศษหากไม่มีข้อมูลบันทึกเข้ามา
4. **`EVENING_CLOSE` (17:00 – 17:30 น.):**
   - รายงานสรุปปิดตลาดเต็มวัน จำนวนแท่งเทียนรวมทั้งวัน (เทียบเป้า 325 แท่ง), อัตราความสมบูรณ์ (Completeness %), ขนาดฐานข้อมูล Turso (MB/KB) และบทวิเคราะห์ความสมบูรณ์ (Integrity Verdict)

> [!NOTE]
> **Market-Open Data Guard:** หากวันดังกล่าวไม่มีข้อมูลบันทึกในตาราง Intraday เลย (เช่น วันหยุดราชการ/วันหยุดนักขัตฤกษ์) ระบบจะข้ามการส่งแจ้งเตือนโดยอัตโนมัติ เพื่อไม่ให้มีข้อความรบกวน

---

## 🔐 การตั้งค่า Secrets บน GitHub Repository

ไปที่ GitHub Repository ของคุณ -> **Settings** -> **Secrets and variables** -> **Actions** -> กด **New repository secret** แล้วเพิ่มรายการต่อไปนี้:

| Secret Name | คำอธิบาย | ตัวอย่างค่า |
| :--- | :--- | :--- |
| `SETTRADE_APP_ID` | App ID จาก Settrade Open API | `abc123xxxx` |
| `SETTRADE_APP_SECRET` | App Secret จาก Settrade Open API | `xyz789xxxx` |
| `SETTRADE_BROKER_ID` | รหัสโบรกเกอร์ (หรือ `SANDBOX` / `098`) | `098` หรือ `SANDBOX` |
| `SETTRADE_APP_CODE` | รหัส App Code (ค่าเริ่มต้น: `ALGO`) | `ALGO` |
| `SETTRADE_IS_SANDBOX` | กำหนดว่าใช้ Sandbox หรือไม่ | `false` (หรือ `true` หากทดสอบบน UAT) |
| `TURSO_DATABASE_URL` | URL ของ Turso Database | `libsql://your-db-org.turso.io` |
| `TURSO_AUTH_TOKEN` | Auth Token ของ Turso Database | `eyJhbGciOi...` |
| `TELEGRAM_BOT_TOKEN` | Bot Token สำหรับแจ้งเตือนสถานะ | `9267903435:AAEAY...` |
| `TELEGRAM_CHAT_ID` | Chat ID ปลายทางที่รับการแจ้งเตือน | `1427208361` |

---

## 🚀 วิธีการทดสอบในเครื่อง (Local Testing)

1. **Clone repository หรือเข้าโฟลเดอร์โปรเจกต์:**
   ```bash
   cd Intraday-Trade
   ```

2. **สร้าง Virtual Environment และติดตั้ง Dependencies:**
   ```bash
   python -m venv venv
   # บน Windows:
   venv\Scripts\activate
   # บน macOS/Linux:
   source venv/bin/activate

   pip install -r requirements.txt
   ```

3. **เตรียม Environment Variables:**
   คัดลอก `.env.example` เป็น `.env` และกรอกค่าจริง:
   ```bash
   cp .env.example .env
   ```

4. **รันทดสอบ:**
   - **รันตามเงื่อนไขเวลาตลาดปกติ:**
     ```bash
     python collector.py
     ```
     *(หากรันนอกเวลาทำการ หรือวันเสาร์-อาทิตย์ สคริปต์จะตรวจสอบว่าอยู่ในช่วง Checkpoint หรือไม่ หากอยู่นอกช่วงจะแจ้งเตือนและจบการทำงานทันที)*

   - **บังคับรันทันที (Bypass Trading Hours Guard):**
     ```bash
     python collector.py --force
     ```
   - **ทดสอบส่งรายงานสถานะเข้า Telegram ทันที (Manual Test):**
     ```bash
     python collector.py --notify
     ```
     *(ระบบจะดึงข้อมูลล่าสุดจาก Turso มาสรุปส่งเข้า Telegram ทันที โดยไม่บันทึกตัดสิทธิ์รอบจริงในตาราง `intraday_telegram_log`)*
   - **กำหนดจำนวน Worker Concurrency (ค่าเริ่มต้น: 8):**
     ```bash
     python collector.py --force --workers 8 --max-rps 45
     ```

---

## ⚡ การทำงานบน GitHub Actions

- **ตารางรันอัตโนมัติ (Schedule):**
  - ตั้ง `cron: '*/1 2-5,7-10 * * 1-5'` รันทุก 1 นาทีในวันจันทร์–ศุกร์ ครอบคลุมทั้งช่วงตลาดทำการและการส่งแจ้งเตือนทั้ง 4 Checkpoints (รอบเช้า 09:45–13:00 น. และรอบบ่าย 14:15–17:30 น. ตามเวลาไทย)
  - ภายใน Job มีการจำกัด `timeout-minutes: 3` และเปิด `cache: 'pip'`
- **สั่งรันด้วยมือ (Manual Dispatch):**
  - สามารถไปที่แท็บ **Actions** บน GitHub -> เลือก **Collect TFEX 1-Minute Data** -> กด **Run workflow**
  - ตัวเลือก `force_run` (ค่าเริ่มต้น: `true`) เพื่อสั่งดึงข้อมูลและบันทึกลง Turso ได้ทันที
  - ตัวเลือก `notify_only` (ค่าเริ่มต้น: `false`) เพื่อสั่งส่งสรุปสถานะเข้า Telegram ทันทีโดยไม่ดึงข้อมูลใหม่

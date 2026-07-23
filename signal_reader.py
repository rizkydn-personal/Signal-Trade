import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Dict, Any
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from google import genai
from google.genai import types
from telethon import TelegramClient, events

load_dotenv()

# ============== KONFIGURASI TELEGRAM ==============
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
SESSION_NAME = "signal_reader_session"

# Kosongkan set ini ({}) supaya membaca SEMUA grup/channel tempat akun ini berada.
ALLOWED_CHAT_IDS = set()

# Folder output
OUTPUT_DIR = r"C:\Users\Administrator\AppData\Roaming\MetaQuotes\Terminal\D0E8209F77C8CF37AD8BF550E51FF075\MQL5\Files"
LOG_FILE = os.path.join(OUTPUT_DIR, "messages_log.jsonl")
SIGNAL_JSON = os.path.join(OUTPUT_DIR, "orders.json")

# ============== KONFIGURASI GEMINI ==============
# Gunakan model gemini-3.1-flash-lite
GEMINI_MODEL = "gemini-3.1-flash-lite"
gemini_client = genai.Client()

# ============== KONFIGURASI LOT ==============
MIN_LOT = 0.01           # Minimum lot
MAX_LOT = 0.05            # Maximum lot (diubah dari 10.0 menjadi 0.05)
LOT_STEP = 0.01          # Step lot
DEFAULT_LOT = 0.02       # Default lot
TOTAL_LOT_PER_ENTRY = 0.02  # Total lot per entry

def normalize_lot(lot: float) -> float:
    """
    Normalisasi lot agar sesuai dengan aturan MetaTrader:
    - Minimal 0.01
    - Maksimal 0.05
    - Kelipatan 0.01 (LOT_STEP)
    """
    if lot <= 0:
        lot = DEFAULT_LOT
    
    # Batasi ke minimum dan maximum
    lot = max(MIN_LOT, min(lot, MAX_LOT))
    
    # Bulatkan ke kelipatan LOT_STEP terdekat
    lot = round(lot / LOT_STEP) * LOT_STEP
    
    # Pastikan tidak kurang dari minimum
    if lot < MIN_LOT:
        lot = MIN_LOT
    
    # Pastikan tidak lebih dari maximum
    if lot > MAX_LOT:
        lot = MAX_LOT
    
    return lot

def calculate_lot_per_order(total_lot: float, num_tp_levels: int) -> float:
    """
    Hitung lot per order dengan pembagian merata
    Memastikan hasilnya valid untuk MetaTrader
    """
    if num_tp_levels <= 0:
        return DEFAULT_LOT
    
    # Bagi total lot dengan jumlah TP
    lot_per_order = total_lot / num_tp_levels
    
    # Normalisasi agar valid
    lot_per_order = normalize_lot(lot_per_order)
    
    # Jika hasil pembagian < MIN_LOT, gunakan MIN_LOT
    if lot_per_order < MIN_LOT:
        lot_per_order = MIN_LOT
    
    return lot_per_order

# ============== KONFIGURASI JARAK MINIMUM SL/TP ==============
MIN_STOP_DISTANCE = {
    "XAUUSD": 0.50,      # 50 poin untuk XAUUSD
    "EURUSD": 0.0010,    # 10 poin untuk EURUSD
    "GBPUSD": 0.0010,    # 10 poin untuk GBPUSD
    "USDJPY": 0.10,      # 10 poin untuk USDJPY
    "DEFAULT": 0.50      # Default 50 poin
}

def get_min_stop_distance(pair: str) -> float:
    """Mendapatkan jarak minimum SL/TP untuk pair tertentu"""
    return MIN_STOP_DISTANCE.get(pair.upper(), MIN_STOP_DISTANCE["DEFAULT"])

# ============== SKEMA PYDANTIC ==============
class TradeOrder(BaseModel):
    pair: str = Field(description="Simbol trading huruf besar, contoh: XAUUSD, EURUSD")
    action: str = Field(description="Hanya boleh teks kaku 'BUY' or 'SELL'")
    entry: float = Field(description="Harga entry utama berupa angka desimal tunggal")
    sl: float = Field(description="Harga Stop Loss berupa angka desimal")
    tp: float = Field(description="Harga Take Profit berupa angka desimal tunggal")
    lot: float = Field(description="Ukuran lot untuk posisi ini (minimal 0.01, maksimal 0.05, kelipatan 0.01)")
    order_type: str = Field(default="LIMIT", description="Tipe order: 'LIMIT' or 'MARKET'")
    entry_description: str = Field(default="", description="Deskripsi tambahan untuk entry")

class TradingSignalResponse(BaseModel):
    orders: List[TradeOrder]

# ============== SYSTEM INSTRUCTION GEMINI ==============
GEMINI_SYSTEM_INSTRUCTION = """
Anda adalah parser sinyal trading. Tugas Anda memproses sinyal dengan format APAPUN.

**ATURAN PARSING:**
1. Ambil ACTION: BUY atau SELL
2. Ambil ENTRY dari berbagai format
3. Ambil STOP LOSS (SL)
4. Ambil TAKE PROFIT (TP) - bisa multiple
5. DETEKSI PAIR: default XAUUSD jika tidak ada
6. DETEKSI ORDER TYPE: MARKET jika ada "instant/now", else LIMIT

**ATURAN LOT:**
- Total lot per entry: 0.02
- Jika multiple TP, bagi lot merata (minimal 0.01 per order)
- MAKSIMAL LOT PER ORDER: 0.05

**VALIDASI SL/TP UNTUK MENGHINDARI "INVALID STOPS":**
- BUY: SL harus < Entry (minimal jarak 0.50 untuk XAUUSD)
- BUY: TP harus > Entry (minimal jarak 0.50 untuk XAUUSD)
- SELL: SL harus > Entry (minimal jarak 0.50 untuk XAUUSD)
- SELL: TP harus < Entry (minimal jarak 0.50 untuk XAUUSD)
- Jika jarak SL/TP terlalu dekat, BULATKAN ke harga yang valid

**OUTPUT:** JSON sesuai skema. Jika tidak valid, keluarkan {"orders": []}
"""

# ============== FILTER SINYAL ==============
class SignalParser:
    """Kelas untuk parsing sinyal dengan berbagai format"""
    
    def __init__(self):
        self.pair_keywords = {
            'xauusd': ['xau', 'gold', 'xauusd', 'xau/usd', 'emas'],
            'eurusd': ['eur', 'eurusd', 'eur/usd'],
            'gbpusd': ['gbp', 'gbpusd', 'gbp/usd'],
            'usdjpy': ['jpy', 'usdjpy', 'usd/jpy'],
        }
        self.default_pair = "XAUUSD"
    
    def detect_pair(self, text: str) -> str:
        text_lower = text.lower()
        for pair, keywords in self.pair_keywords.items():
            for keyword in keywords:
                if keyword in text_lower:
                    return pair.upper()
        return self.default_pair
    
    def detect_action(self, text: str) -> Optional[str]:
        text_lower = text.lower()
        
        sell_patterns = ['sell', 'short', 'instant sell', 'sell now']
        for pattern in sell_patterns:
            if pattern in text_lower:
                return 'SELL'
        
        buy_patterns = ['buy', 'long', 'instant buy', 'buy now']
        for pattern in buy_patterns:
            if pattern in text_lower:
                return 'BUY'
        
        return None
    
    def detect_order_type(self, text: str) -> str:
        text_lower = text.lower()
        if any(word in text_lower for word in ['instant', 'market', 'now', 'immediate']):
            return 'MARKET'
        return 'LIMIT'
    
    def parse_entry_prices(self, text: str) -> List[Dict[str, Any]]:
        """
        Ambil harga entry dari baris yang mengandung kata aksi (BUY/SELL/LONG/SHORT).
        Mendukung berbagai format penulisan, contoh:
          - "BUY XAUUSD 4009/4005"
          - "GOLD BUY NOW : 4020 - 4017"
          - "XAU/USD SELL 4024/4029"
          - "XAUUSD BUY NOW 4018,4015"
        Pendekatan: cari baris pertama yang mengandung kata aksi, lalu ambil
        semua angka pada baris tsb (nama pair seperti XAUUSD/XAU/USD/GOLD
        tidak mengandung digit sehingga aman diabaikan).
        """
        entries = []
        order_type = self.detect_order_type(text)
        action_words = ('buy', 'sell', 'long', 'short')

        lines = [ln for ln in text.splitlines() if ln.strip()]
        entry_line = None
        for line in lines:
            if any(w in line.lower() for w in action_words):
                entry_line = line
                break
        if entry_line is None and lines:
            entry_line = lines[0]

        def extract_numbers(s: str) -> List[float]:
            # Hanya titik (.) yang dianggap desimal. Koma dianggap sebagai
            # pemisah antar harga (contoh: "4018,4015" = dua harga terpisah),
            # bukan sebagai desimal.
            found = re.findall(r'\d+(?:\.\d+)?', s)
            nums = []
            for n in found:
                try:
                    nums.append(float(n))
                except ValueError:
                    continue
            return nums

        numbers = extract_numbers(entry_line) if entry_line else []

        if len(numbers) >= 2:
            price1, price2 = numbers[0], numbers[1]
            avg_price = round((price1 + price2) / 2, 2)
            entries.append({
                'price': avg_price,
                'type': 'range',
                'description': f'{price1}-{price2}',
                'order_type': order_type
            })
        elif len(numbers) == 1:
            entries.append({
                'price': numbers[0],
                'type': 'single',
                'description': str(numbers[0]),
                'order_type': order_type
            })
        else:
            # Fallback terakhir: cari angka yang masuk akal di seluruh teks
            all_numbers = extract_numbers(text)
            for price in all_numbers:
                if price > 0:
                    entries.append({
                        'price': price,
                        'type': 'single',
                        'description': str(price),
                        'order_type': order_type
                    })
                    break

        return entries
    
    def parse_sl_price(self, text: str) -> Optional[float]:
        sl_patterns = [
            # Format gabungan "SL.TP 4006.00" / "SL TP 4006.00" (typo umum,
            # dimaksudkan sebagai satu nilai SL) - dicek lebih dulu supaya
            # tidak salah tertangkap oleh pattern SL biasa di bawah.
            r'SL[.\s]*TP\s*[-:]?\s*([\d.]+)',
            r'(?:SL|Stop Loss|Stoploss)\s*[-:]\s*([\d.]+)',
            r'SL\s*[-:]?\s*([\d.]+)',
            r'stop\s+loss\s*[-:]\s*([\d.]+)',
            r'SL_\s*([\d.]+)',
            r'sl\s*=\s*([\d.]+)',
        ]
        
        for pattern in sl_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    return float(match.group(1))
                except ValueError:
                    continue
        return None
    
    def parse_tp_levels(self, text: str) -> List[float]:
        """
        Ambil semua level Take Profit, per baris. Mendukung:
          - "TP¹ 4012" / "TP² : 4026" (superscript, dengan atau tanpa ':')
          - "Tp1  4022" (nomor level menempel langsung setelah TP, tanpa spasi)
          - "TP1: 4023" / "TP - 4012" / "take profit 4012"
        Superscript (¹²³...) tidak dianggap digit oleh regex \\d, jadi baris
        seperti "TP¹ 4012" hanya menghasilkan satu angka (4012). Untuk baris
        seperti "Tp1  4022" akan ada dua angka ("1" dan "4022"); dalam kasus
        itu angka TERAKHIR pada baris dianggap sebagai harga TP (nomor level
        selalu ditulis lebih dulu, harga selalu di akhir baris).
        """
        tp_levels = []

        for line in text.splitlines():
            line_lower = line.lower()
            if not line_lower.strip():
                continue

            # Lewati baris shorthand gabungan "SL.TP xxxx" / "SL TP xxxx" -
            # itu adalah nilai SL, bukan TP (ditangani oleh parse_sl_price).
            if re.search(r'\bsl[.\s]*tp\b', line_lower):
                continue

            if 'tp' not in line_lower and 'take profit' not in line_lower:
                continue

            numbers = re.findall(r'\d+(?:\.\d+)?', line)
            if not numbers:
                continue

            try:
                price = float(numbers[-1])
            except ValueError:
                continue

            if price > 0:
                tp_levels.append(price)

        action = self.detect_action(text)
        if action == 'SELL':
            tp_levels = sorted(set(tp_levels), reverse=True)
        elif action == 'BUY':
            tp_levels = sorted(set(tp_levels))

        return tp_levels
    
    def is_valid_signal(self, text: str) -> Tuple[bool, str]:
        if not text:
            return False, "Text kosong"
        
        action = self.detect_action(text)
        if not action:
            return False, "Tidak ada action (BUY/SELL)"
        
        sl = self.parse_sl_price(text)
        if not sl or sl <= 0:
            return False, "Tidak ada Stop Loss (SL) yang valid"
        
        tp_levels = self.parse_tp_levels(text)
        if not tp_levels:
            return False, "Tidak ada Take Profit (TP) yang valid"
        
        entries = self.parse_entry_prices(text)
        if not entries:
            return False, "Tidak ada harga entry yang valid"
        
        return True, "Valid"
    
    def preprocess_signal(self, text: str) -> Dict[str, Any]:
        return {
            'pair': self.detect_pair(text),
            'action': self.detect_action(text),
            'entries': self.parse_entry_prices(text),
            'sl': self.parse_sl_price(text),
            'tp_levels': self.parse_tp_levels(text),
            'order_type': self.detect_order_type(text),
            'raw_text': text
        }

# ============== INISIALISASI PARSER ==============
parser = SignalParser()

# ============== LOGGING ==============
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("telegram_signal_reader")

def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def write_full_log(entry: dict):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

def write_latest_signal_json(orders_list: list):
    payload = {
        "orders": orders_list,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "order_count": len(orders_list)
    }
    
    with open(SIGNAL_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    
    logger.info("File perintah eksekusi trading diperbarui: %s", SIGNAL_JSON)
    if orders_list:
        logger.info("Parsed orders: %s", json.dumps(payload, ensure_ascii=False, indent=2))

# ============== VALIDASI SL/TP UNTUK MENGHINDARI "INVALID STOPS" ==============
def validate_and_fix_stops(order: dict) -> Tuple[bool, dict]:
    """
    Validasi dan perbaiki SL/TP untuk menghindari "invalid stops"
    Returns: (is_valid, fixed_order)
    """
    entry = order.get("entry", 0)
    sl = order.get("sl", 0)
    tp = order.get("tp", 0)
    action = order.get("action", "").upper()
    pair = order.get("pair", "XAUUSD")
    
    # Dapatkan jarak minimum
    min_distance = get_min_stop_distance(pair)
    
    # Log untuk debugging
    logger.debug(f"Validating: {action} {pair} Entry={entry}, SL={sl}, TP={tp}, MinDist={min_distance}")
    
    # Validasi untuk BUY
    if action == "BUY":
        # SL harus di bawah entry
        if sl >= entry:
            logger.warning(f"BUY: SL ({sl}) harus < Entry ({entry})")
            sl = entry - min_distance
            order['sl'] = sl
            logger.info(f"BUY: SL diperbaiki menjadi {sl}")
        
        # TP harus di atas entry
        if tp <= entry:
            logger.warning(f"BUY: TP ({tp}) harus > Entry ({entry})")
            tp = entry + min_distance
            order['tp'] = tp
            logger.info(f"BUY: TP diperbaiki menjadi {tp}")
        
        # Cek jarak minimum SL
        if (entry - sl) < min_distance:
            logger.warning(f"BUY: Jarak SL terlalu dekat ({entry - sl:.2f} < {min_distance})")
            sl = entry - min_distance
            order['sl'] = sl
            logger.info(f"BUY: SL digeser ke {sl}")
        
        # Cek jarak minimum TP
        if (tp - entry) < min_distance:
            logger.warning(f"BUY: Jarak TP terlalu dekat ({tp - entry:.2f} < {min_distance})")
            tp = entry + min_distance
            order['tp'] = tp
            logger.info(f"BUY: TP digeser ke {tp}")
    
    # Validasi untuk SELL
    elif action == "SELL":
        # SL harus di atas entry
        if sl <= entry:
            logger.warning(f"SELL: SL ({sl}) harus > Entry ({entry})")
            sl = entry + min_distance
            order['sl'] = sl
            logger.info(f"SELL: SL diperbaiki menjadi {sl}")
        
        # TP harus di bawah entry
        if tp >= entry:
            logger.warning(f"SELL: TP ({tp}) harus < Entry ({entry})")
            tp = entry - min_distance
            order['tp'] = tp
            logger.info(f"SELL: TP diperbaiki menjadi {tp}")
        
        # Cek jarak minimum SL
        if (sl - entry) < min_distance:
            logger.warning(f"SELL: Jarak SL terlalu dekat ({sl - entry:.2f} < {min_distance})")
            sl = entry + min_distance
            order['sl'] = sl
            logger.info(f"SELL: SL digeser ke {sl}")
        
        # Cek jarak minimum TP
        if (entry - tp) < min_distance:
            logger.warning(f"SELL: Jarak TP terlalu dekat ({entry - tp:.2f} < {min_distance})")
            tp = entry - min_distance
            order['tp'] = tp
            logger.info(f"SELL: TP digeser ke {tp}")
    
    else:
        return False, order
    
    # Update order dengan nilai yang sudah diperbaiki
    order['sl'] = round(sl, 2)
    order['tp'] = round(tp, 2)
    
    # Final check
    if action == "BUY":
        if sl >= entry or tp <= entry:
            logger.error(f"BUY: Setelah perbaikan masih invalid: SL={sl}, TP={tp}, Entry={entry}")
            return False, order
        if (entry - sl) < min_distance or (tp - entry) < min_distance:
            logger.error(f"BUY: Setelah perbaikan jarak masih terlalu dekat")
            return False, order
    elif action == "SELL":
        if sl <= entry or tp >= entry:
            logger.error(f"SELL: Setelah perbaikan masih invalid: SL={sl}, TP={tp}, Entry={entry}")
            return False, order
        if (sl - entry) < min_distance or (entry - tp) < min_distance:
            logger.error(f"SELL: Setelah perbaikan jarak masih terlalu dekat")
            return False, order
    
    return True, order

# ============== FORMATTER GEMINI ==============
def format_signal_with_gemini(raw_text: str) -> list | None:
    """Memanggil Gemini API untuk memparsing sinyal"""
    try:
        preprocessed = parser.preprocess_signal(raw_text)
        
        if not preprocessed['action']:
            logger.warning("Action tidak ditemukan")
            return []
        
        if not preprocessed['entries']:
            logger.warning("Entry tidak ditemukan")
            return []
        
        if not preprocessed['sl'] or preprocessed['sl'] <= 0:
            logger.warning(f"SL tidak valid: {preprocessed['sl']}")
            return []
        
        if not preprocessed['tp_levels']:
            logger.warning("TP tidak ditemukan")
            return []
        
        num_tp = len(preprocessed['tp_levels'])
        lot_per_order = calculate_lot_per_order(TOTAL_LOT_PER_ENTRY, num_tp)
        min_distance = get_min_stop_distance(preprocessed['pair'])
        
        entry_descriptions = []
        for entry in preprocessed['entries']:
            entry_descriptions.append(f"{entry['price']} ({entry['type']}, {entry['order_type']})")
        
        formatted_text = f"""
Pair: {preprocessed['pair']}
Action: {preprocessed['action']}
Entries: {', '.join(entry_descriptions)}
SL: {preprocessed['sl']}
TP Levels: {', '.join([str(tp) for tp in preprocessed['tp_levels']])}
Order Type: {preprocessed['order_type']}
Lot per order: {lot_per_order:.2f} (MAX LOT: 0.05)
Minimum Stop Distance: {min_distance} (jangan kurang dari ini)

Raw signal: {raw_text}

INSTRUKSI KHUSUS:
1. Buat order terpisah untuk SETIAP entry
2. Untuk SETIAP entry, buat order terpisah untuk SETIAP TP level
3. Gunakan lot = {lot_per_order:.2f} untuk SETIAP order (MAKSIMAL 0.05)
4. Pastikan jarak SL dan TP dari entry minimal {min_distance}
5. Pair default: XAUUSD jika tidak disebutkan
"""
        
        if len(formatted_text) > 3000:
            formatted_text = formatted_text[:3000]
            logger.warning("Teks dipotong karena terlalu panjang")
        
        # Gunakan model gemini-3.1-flash-lite
        response = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=f"Teks Sinyal:\n{formatted_text}",
            config=types.GenerateContentConfig(
                system_instruction=GEMINI_SYSTEM_INSTRUCTION,
                response_mime_type="application/json",
                response_schema=TradingSignalResponse,
                temperature=0.1,
                max_output_tokens=4096,
            ),
        )
        
        data = json.loads(response.text)
        orders = data.get("orders", [])
        
        # Normalisasi lot dan validasi SL/TP
        fixed_orders = []
        for order in orders:
            if 'lot' in order:
                order['lot'] = normalize_lot(order['lot'])
            
            # Validasi dan perbaiki SL/TP
            is_valid, fixed_order = validate_and_fix_stops(order)
            if is_valid:
                fixed_orders.append(fixed_order)
            else:
                logger.warning(f"Order tidak bisa diperbaiki: {order}")
        
        if not fixed_orders:
            logger.info("Tidak ada order yang valid setelah validasi SL/TP")
            return []
        
        logger.info(f"Gemini menghasilkan {len(fixed_orders)} order yang valid")
        return fixed_orders
        
    except json.JSONDecodeError as e:
        logger.error(f"Gagal parse JSON dari Gemini: {e}")
        return []
    except Exception as e:
        logger.error(f"Gagal memformat sinyal lewat Gemini: {e}")
        return []

# ============== VALIDASI ORDER ==============
def validate_orders(orders: list) -> list:
    """Validasi setiap order"""
    if not orders:
        return []
    
    valid_orders = []
    
    for order in orders:
        required_fields = ["pair", "action", "entry", "sl", "tp", "lot"]
        if not all(field in order for field in required_fields):
            logger.warning(f"Order missing required fields: {order}")
            continue
        
        entry = order.get("entry", 0)
        sl = order.get("sl", 0)
        tp = order.get("tp", 0)
        lot = order.get("lot", 0)
        action = order.get("action", "").upper()
        pair = order.get("pair", "XAUUSD")
        
        if entry <= 0 or sl <= 0 or tp <= 0:
            logger.warning(f"Order memiliki nilai <= 0: entry={entry}, sl={sl}, tp={tp}")
            continue
        
        # Normalisasi lot (dengan MAX_LOT = 0.05)
        lot = normalize_lot(lot)
        order['lot'] = lot
        
        # Validasi lot tidak melebihi MAX_LOT
        if lot > MAX_LOT:
            logger.warning(f"Lot {lot} melebihi maksimum {MAX_LOT}, diset ke {MAX_LOT}")
            order['lot'] = MAX_LOT
        
        # Validasi dan perbaiki SL/TP
        is_valid, fixed_order = validate_and_fix_stops(order)
        if not is_valid:
            logger.warning(f"Order tidak valid setelah perbaikan: {order}")
            continue
        
        # Order valid
        valid_orders.append(fixed_order)
        logger.info(f"✅ Order valid: {action} {pair} @ {entry}, SL={sl}, TP={tp}, Lot={lot:.2f}")
    
    return valid_orders

# ============== TELETHON CLIENT ==============
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

@client.on(events.NewMessage())
async def handle_message(event):
    if not event.is_group and not event.is_channel:
        return
        
    raw_text = event.raw_text
    if not raw_text:
        return
        
    chat_id = event.chat_id
    if ALLOWED_CHAT_IDS and chat_id not in ALLOWED_CHAT_IDS:
        return
    
    is_valid, reason = parser.is_valid_signal(raw_text)
    if not is_valid:
        logger.info(f"❌ Sinyal tidak valid: {reason}")
        return
    
    logger.info(f"✅ Sinyal valid terdeteksi di chat {chat_id}")
    
    loop = asyncio.get_event_loop()
    parsed_orders = await loop.run_in_executor(None, format_signal_with_gemini, raw_text)
    
    if not parsed_orders:
        logger.info("⚠️ Gemini tidak menghasilkan order yang valid.")
        return
    
    valid_orders = validate_orders(parsed_orders)
    
    if not valid_orders:
        logger.info("❌ Tidak ada order yang valid setelah validasi.")
        return
    
    chat = await event.get_chat()
    chat_title = getattr(chat, "title", None) or str(chat_id)
    
    sender = await event.get_sender()
    username = getattr(sender, "username", None) or getattr(sender, "first_name", None) or str(getattr(sender, "id", "unknown"))
    
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "chat_id": chat_id,
        "chat_title": chat_title,
        "username": username,
        "raw_text": raw_text,
        "orders": valid_orders,
        "status": "VALID",
        "order_count": len(valid_orders)
    }
    
    ensure_output_dir()
    write_full_log(entry)
    write_latest_signal_json(valid_orders)

# ============== MAIN ==============
def main():
    if not API_ID or not API_HASH:
        raise SystemExit("Error: API_ID dan API_HASH wajib diisi di file .env")
        
    if not os.environ.get("GEMINI_API_KEY"):
        raise SystemExit("Error: GEMINI_API_KEY wajib diisi di file .env")
        
    ensure_output_dir()
    logger.info("Menghubungkan ke Telegram userbot...")
    
    client.start()
    logger.info("="*60)
    logger.info("USERBOT AKTIF! Mendengarkan sinyal trading...")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info(f"Signal file: {SIGNAL_JSON}")
    logger.info(f"Gemini Model: {GEMINI_MODEL}")
    logger.info(f"Lot settings: MIN={MIN_LOT}, MAX={MAX_LOT}, STEP={LOT_STEP}")
    logger.info(f"Min Stop Distance: {MIN_STOP_DISTANCE}")
    logger.info("="*60)
    
    client.run_until_disconnected()

if __name__ == "__main__":
    main()
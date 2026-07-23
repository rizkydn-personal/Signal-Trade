//+------------------------------------------------------------------+
//|                                         GeminiOrderExecutor.mq5  |
//|                                  Copyright 2026, Bot Auto Trading|
//+------------------------------------------------------------------+
#property copyright "Copyright 2026"
#property link      "https://google.com"
#property version   "1.00"

// Sertakan library resmi bawaan MT5 untuk eksekusi trade
#include <Trade\Trade.mqh>
CTrade trade;

// Input parameter yang bisa diatur dari panel MT5
input string   JsonFileName   = "orders.json"; // Nama file yang ditulis oleh Python
input string   FailedOrdersLogFile = "failed_orders.json"; // Log kegagalan eksekusi order (dibaca oleh dashboard /admin)
input int      TimerSeconds   = 1;             // Interval cek file (detik)
input int      DefaultSlippage= 30;            // Toleransi harga melesat (points)
input int      TrailingStop   = 200;           // Jarak Trailing Stop jika TP=0 (20 pips / 200 points)
input string   SymbolSuffix   = "";            // (Opsional) paksa suffix tertentu, mis. ".m". Kosongkan saja - EA akan auto-detect (cent/pro/dll)

// Cache hasil auto-detect nama simbol broker, supaya tidak perlu scan ulang setiap order
string g_cache_base[];
string g_cache_resolved[];

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
   // Aktifkan timer sistem setiap X detik
   EventSetTimer(TimerSeconds);
   Print("EA Gemini Order Executor Aktif. Menunggu file: ", JsonFileName);
   return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   // Matikan timer saat EA dicopot
   EventKillTimer();
}

//+------------------------------------------------------------------+
//| Expert tick function (Berjalan setiap ada perubahan harga)       |
//+------------------------------------------------------------------+
void OnTick()
{
   // Jalankan fungsi trailing stop dinamis untuk posisi yang tidak memiliki TP
   ApplyTrailingStop();
}

//+------------------------------------------------------------------+
//| Timer function (Berjalan berulang berdasarkan interval waktu)   |
//+------------------------------------------------------------------+
void OnTimer()
{
   // Cek apakah file JSON dari Python sudah tersedia di folder MQL5/Files/
   if(FileIsExist(JsonFileName, 0))
   {
      Print("File sinyal baru terdeteksi! Memulai parsing...");
      ExecuteOrdersFromJson();
   }
}

//+------------------------------------------------------------------+
//| Fungsi membaca JSON secara manual (Simpel & Kaku untuk Pemula)   |
//+------------------------------------------------------------------+
void ExecuteOrdersFromJson()
{
   int file_handle = FileOpen(JsonFileName, FILE_READ|FILE_TXT|FILE_ANSI);
   if(file_handle == INVALID_HANDLE)
   {
      Print("Gagal membuka file JSON.");
      return;
   }

   string json_content = "";
   while(!FileIsEnding(file_handle))
   {
      json_content += FileReadString(file_handle);
   }
   FileClose(file_handle);

   // Hapus file segera agar tidak tereksekusi dua kali pada timer berikutnya
   FileDelete(JsonFileName, 0);

   // --- LOGIKA PARSING TEKS JSON SEDERHANA ---
   // Karena format JSON dari Python kaku dan flat, kita bisa gunakan pencarian string dasar
   string search_key = "\"pair\":";
   int offset = 0;

   while(true)
   {
      int pos = StringFind(json_content, search_key, offset);
      if(pos == -1) break; // Tidak ada order lagi di dalam JSON

      // Cari batas kurung kurawal satu objek order { ... }
      // Cari "{" mulai dari offset (setelah objek sebelumnya berakhir), bukan pos-20 yang rapuh
      int start_obj = StringFind(json_content, "{", offset);
      int end_obj = StringFind(json_content, "}", pos);
      if(start_obj == -1 || end_obj == -1) break;

      string order_block = StringSubstr(json_content, start_obj, end_obj - start_obj + 1);
      offset = end_obj + 1;

      // Ekstrak nilai secara manual dari teks blok JSON
      string pair       = ExtractJsonValue(order_block, "pair");
      string action     = ExtractJsonValue(order_block, "action");
      string order_type = ExtractJsonValue(order_block, "order_type");
      double entry  = StringToDouble(ExtractJsonValue(order_block, "entry"));
      double sl     = StringToDouble(ExtractJsonValue(order_block, "sl"));
      double tp     = StringToDouble(ExtractJsonValue(order_block, "tp"));
      double lot    = StringToDouble(ExtractJsonValue(order_block, "lot"));

      // Jika lot gagal dibaca atau 0, pasang lot aman standar
      if(lot <= 0) lot = 0.01;

      // Default ke MARKET jika field order_type kosong/tidak terbaca
      if(order_type == "") order_type = "MARKET";

      // Eksekusi Order ke Broker (otomatis pilih Market / Limit / Stop yang sesuai)
      SubmitOrder(pair, action, order_type, lot, entry, sl, tp);
   }
}

//+------------------------------------------------------------------+
//| Fungsi Pembantu untuk Mengambil Nilai dari String JSON           |
//+------------------------------------------------------------------+
string ExtractJsonValue(string block, string key)
{
   int key_pos = StringFind(block, "\"" + key + "\"", 0);
   if(key_pos == -1) return "";

   int colon_pos = StringFind(block, ":", key_pos);
   int comma_pos = StringFind(block, ",", colon_pos);
   if(comma_pos == -1) comma_pos = StringFind(block, "}", colon_pos);

   string val = StringSubstr(block, colon_pos + 1, comma_pos - colon_pos - 1);
   
   // Bersihkan karakter sampah seperti spasi, tanda kutip, dan newline
   StringReplace(val, "\"", "");
   StringReplace(val, " ", "");
   StringReplace(val, "\r", "");
   StringReplace(val, "\n", "");
   return val;
}

//+------------------------------------------------------------------+
//| Deteksi otomatis nama simbol asli di broker dari kode pair sinyal|
//| Contoh: sinyal "XAUUSD" -> broker cent bisa jadi "XAUUSDc",     |
//| broker lain "XAUUSD.m", "XAUUSDm", dst. Hasil di-cache agar     |
//| pencarian hanya dilakukan sekali per pair.                       |
//+------------------------------------------------------------------+
string ResolveBrokerSymbol(string base_pair)
{
   // 1. Cek cache dulu
   for(int i = 0; i < ArraySize(g_cache_base); i++)
   {
      if(g_cache_base[i] == base_pair)
         return g_cache_resolved[i];
   }

   string resolved = "";

   // 2. Coba nama persis apa adanya
   if(SymbolSelect(base_pair, true))
      resolved = base_pair;

   // 3. Coba dengan SymbolSuffix manual jika diisi user
   if(resolved == "" && SymbolSuffix != "")
   {
      string with_suffix = base_pair + SymbolSuffix;
      if(SymbolSelect(with_suffix, true))
         resolved = with_suffix;
   }

   // 4. Auto-scan SEMUA simbol yang disediakan broker (bukan cuma yang ada di Market Watch)
   //    Cocokkan simbol yang namanya DIAWALI kode pair, contoh: XAUUSDc, XAUUSD.m, XAUUSDm
   if(resolved == "")
   {
      string base_upper = base_pair;
      StringToUpper(base_upper);

      int total = SymbolsTotal(false);
      for(int i = 0; i < total; i++)
      {
         string sym = SymbolName(i, false);
         string sym_upper = sym;
         StringToUpper(sym_upper);

         if(StringFind(sym_upper, base_upper) == 0)
         {
            if(SymbolSelect(sym, true))
            {
               resolved = sym;
               break;
            }
         }
      }
   }

   // Simpan ke cache (walau kosong / tidak ketemu, supaya tidak scan berulang tiap order)
   int n = ArraySize(g_cache_base);
   ArrayResize(g_cache_base, n + 1);
   ArrayResize(g_cache_resolved, n + 1);
   g_cache_base[n] = base_pair;
   g_cache_resolved[n] = resolved;

   if(resolved != "")
      Print("🔎 Simbol '", base_pair, "' terdeteksi sebagai '", resolved, "' di broker ini");

   return resolved;
}

//+------------------------------------------------------------------+
//| Fungsi Kirim Perintah Transaksi ke Server Broker                 |
//| Menangani MARKET (Buy/Sell instan) maupun LIMIT (pending order)  |
//| Untuk LIMIT, tipe order (Limit/Stop) dipilih otomatis berdasarkan|
//| posisi harga entry terhadap harga pasar saat ini agar tidak      |
//| memicu error "invalid order type" dari broker.                  |
//+------------------------------------------------------------------+
void SubmitOrder(string symbol_raw, string action, string order_type, double volume,
                  double entry_price, double stoploss, double takeprofit)
{
   // Bersihkan string dari spasi/karakter tersembunyi & samakan huruf besar
   StringTrimLeft(action);   StringTrimRight(action);   StringToUpper(action);
   StringTrimLeft(order_type); StringTrimRight(order_type); StringToUpper(order_type);
   StringTrimLeft(symbol_raw); StringTrimRight(symbol_raw);

   if(action != "BUY" && action != "SELL")
   {
      Print("❌ Action tidak valid: '", action, "' - order dilewati");
      LogFailedOrder(symbol_raw, action, order_type, volume, stoploss, takeprofit, -1, "Action tidak valid: '" + action + "'");
      return;
   }

   string symbol = ResolveBrokerSymbol(symbol_raw);

   // Pastikan simbol ketemu & bisa dipilih di Market Watch, jika tidak broker akan menolak order
   if(symbol == "")
   {
      string reason = "Simbol untuk pair '" + symbol_raw + "' tidak ditemukan di broker ini";
      Print("❌ Simbol untuk pair '", symbol_raw, "' tidak ditemukan di broker ini - order dilewati. ",
            "Simbol yang tersedia mungkin punya nama berbeda; cek Market Watch broker kamu.");
      LogFailedOrder(symbol_raw, action, order_type, volume, stoploss, takeprofit, -1, reason);
      return;
   }

   int    digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   double ask    = SymbolInfoDouble(symbol, SYMBOL_ASK);
   double bid    = SymbolInfoDouble(symbol, SYMBOL_BID);

   if(ask <= 0 || bid <= 0)
   {
      string reason = "Harga simbol '" + symbol + "' tidak valid (Ask/Bid = 0)";
      Print("❌ Harga simbol '", symbol, "' tidak valid (Ask/Bid = 0) - order dilewati");
      LogFailedOrder(symbol, action, order_type, volume, stoploss, takeprofit, -1, reason);
      return;
   }

   // Normalisasi harga sesuai jumlah digit simbol agar tidak ditolak broker
   stoploss    = NormalizeDouble(stoploss, digits);
   takeprofit  = NormalizeDouble(takeprofit, digits);
   entry_price = NormalizeDouble(entry_price, digits);

   // Normalisasi volume sesuai batas min/max/step simbol di broker ini
   // (beda broker/tipe akun bisa punya aturan lot berbeda, terutama akun cent)
   double vol_min  = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
   double vol_max  = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
   double vol_step = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);

   if(vol_step > 0)
      volume = MathRound(volume / vol_step) * vol_step;
   if(vol_min > 0 && volume < vol_min)
      volume = vol_min;
   if(vol_max > 0 && volume > vol_max)
      volume = vol_max;
   volume = NormalizeDouble(volume, 2);

   trade.SetDeviationInPoints(DefaultSlippage);

   bool result = false;

   if(order_type == "MARKET")
   {
      if(action == "BUY")
         result = trade.Buy(volume, symbol, ask, stoploss, takeprofit, "Gemini AI Signal");
      else
         result = trade.Sell(volume, symbol, bid, stoploss, takeprofit, "Gemini AI Signal");
   }
   else // LIMIT -> pilih otomatis Buy/Sell Limit atau Buy/Sell Stop, atau market jika harga sudah tepat
   {
      if(action == "BUY")
      {
         if(entry_price == ask)
            result = trade.Buy(volume, symbol, ask, stoploss, takeprofit, "Gemini AI Signal");
         else if(entry_price < ask)
            result = trade.BuyLimit(volume, entry_price, symbol, stoploss, takeprofit, ORDER_TIME_GTC, 0, "Gemini AI Signal");
         else
            result = trade.BuyStop(volume, entry_price, symbol, stoploss, takeprofit, ORDER_TIME_GTC, 0, "Gemini AI Signal");
      }
      else // SELL
      {
         if(entry_price == bid)
            result = trade.Sell(volume, symbol, bid, stoploss, takeprofit, "Gemini AI Signal");
         else if(entry_price > bid)
            result = trade.SellLimit(volume, entry_price, symbol, stoploss, takeprofit, ORDER_TIME_GTC, 0, "Gemini AI Signal");
         else
            result = trade.SellStop(volume, entry_price, symbol, stoploss, takeprofit, ORDER_TIME_GTC, 0, "Gemini AI Signal");
      }
   }

   if(result)
      Print("✅ Order terkirim: ", action, " ", order_type, " ", symbol,
            " Lot=", volume, " Entry=", entry_price, " SL=", stoploss, " TP=", takeprofit);
   else
   {
      int retcode = (int)trade.ResultRetcode();
      string reason = trade.ResultRetcodeDescription();
      Print("❌ Order GAGAL: ", action, " ", order_type, " ", symbol,
            " | Retcode=", retcode,
            " (", reason, ")",
            " | Comment=", trade.ResultComment());
      LogFailedOrder(symbol, action, order_type, volume, stoploss, takeprofit, retcode, reason);
   }
}

//+------------------------------------------------------------------+
//| Catat kegagalan eksekusi order ke file JSON (dibaca oleh         |
//| dashboard Python di halaman /admin, satu baris = satu kejadian). |
//+------------------------------------------------------------------+
void LogFailedOrder(string symbol, string action, string order_type, double volume, double sl, double tp, int retcode, string reason)
{
   int handle = FileOpen(FailedOrdersLogFile, FILE_READ | FILE_WRITE | FILE_TXT | FILE_ANSI | FILE_SHARE_READ | FILE_SHARE_WRITE);
   if(handle == INVALID_HANDLE)
   {
      Print("Gagal membuka file log kegagalan order: ", FailedOrdersLogFile, " (error ", GetLastError(), ")");
      return;
   }

   FileSeek(handle, 0, SEEK_END);

   // Bersihkan tanda kutip di dalam reason agar JSON tetap valid
   string safe_reason = reason;
   StringReplace(safe_reason, "\"", "'");

   string time_str = TimeToString(TimeCurrent(), TIME_DATE | TIME_SECONDS);
   string line = StringFormat(
      "{\"time\":\"%s\",\"symbol\":\"%s\",\"action\":\"%s %s\",\"volume\":%.2f,\"sl\":%.5f,\"tp\":%.5f,\"retcode\":%d,\"reason\":\"%s\"}",
      time_str, symbol, action, order_type, volume, sl, tp, retcode, safe_reason
   );

   FileWriteString(handle, line + "\r\n");
   FileClose(handle);
}

//+------------------------------------------------------------------+
//| Logika Trailing Stop Otomatis Jika Sinyal Berbobot TP = 0.0      |
//+------------------------------------------------------------------+
void ApplyTrailingStop()
{
   if(TrailingStop <= 0) return;

   // Periksa seluruh posisi trading yang sedang aktif terbuka di akun
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      if(PositionGetSymbol(i) != _Symbol) continue;
      
      // Pastikan order ini adalah milik EA ini
      if(PositionGetString(POSITION_COMMENT) != "Gemini AI Signal") continue;

      ulong  ticket = PositionGetInteger(POSITION_TICKET);
      double current_sl = PositionGetDouble(POSITION_SL);
      double current_tp = PositionGetDouble(POSITION_TP);
      
      // HANYA jalankan trailing stop jika posisi ini dikirim tanpa target TP (TP = 0)
      if(current_tp > 0) continue; 

      if(PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY)
      {
         double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
         // Jika harga sudah naik melebihi jarak trailing stop dari SL saat ini
         if(bid - PositionGetDouble(POSITION_PRICE_OPEN) > TrailingStop * _Point)
         {
            if(current_sl < bid - TrailingStop * _Point)
            {
               trade.PositionModify(ticket, bid - TrailingStop * _Point, 0);
            }
         }
      }
      else if(PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_SELL)
      {
         double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
         if(PositionGetDouble(POSITION_PRICE_OPEN) - ask > TrailingStop * _Point)
         {
            if(current_sl > ask + TrailingStop * _Point || current_sl == 0)
            {
               trade.PositionModify(ticket, ask + TrailingStop * _Point, 0);
            }
         }
      }
   }
}
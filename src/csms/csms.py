# src/csms/csms.py
import asyncio
import logging
import websockets
import aiohttp
from datetime import datetime, timezone, timedelta
from aiohttp import web
from ocpp.v16 import ChargePoint as BaseChargePoint
from ocpp.v16 import call_result, call
from ocpp.routing import on
from ocpp.v16.enums import RegistrationStatus

logging.basicConfig(level=logging.INFO)

connected_cps = {}
connector_status = {}
transaction_data = {}
transaction_history = []


class ChargePoint(BaseChargePoint):

    @on("BootNotification")
    async def on_boot_notification(self, charge_point_vendor, charge_point_model, **kwargs):
        logging.info(f"🔋 BootNotification diterima dari {self.id} | Vendor={charge_point_vendor}, Model={charge_point_model}")
        return call_result.BootNotification(
            current_time=datetime.now(timezone.utc).isoformat(),
            interval=10,
            status=RegistrationStatus.accepted
        )

    @on("Heartbeat")
    async def on_heartbeat(self):
        logging.info(f"💓 Heartbeat diterima dari {self.id}")
        return call_result.Heartbeat(current_time=datetime.now(timezone.utc).isoformat())

    @on("Authorize")
    async def on_authorize(self, id_tag):
        logging.info(f"🔑 Authorize.req diterima dari {self.id} | idTag={id_tag}")
        expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        return call_result.Authorize(
            id_tag_info = {
                "status": "Accepted",
                "expiryDate": expiry.isoformat(),
                "parentIdTag": None
            }
        )

    @on("StartTransaction")
    async def on_start_transaction(self, connector_id, id_tag, timestamp, meter_start, **kwargs):
        # CSMS memberikan transaction_id (contoh: timestamp int). CP akan menerima ini di StartTransaction.conf
        transaction_id = int(datetime.now(timezone.utc).timestamp())
        transaction_data[transaction_id] = {
            "cpId": self.id,
            "idTag": id_tag,
            "connectorId": connector_id,
            "start_time": timestamp,
            "meter_start": meter_start,
            "energy": 0,
            "last_update": timestamp
        }
        logging.info(
            f"⚡ StartTransaction.req dari {self.id} | idTag={id_tag}, connectorId={connector_id}, meterStart={meter_start}, transactionId={transaction_id}"
        )
        return call_result.StartTransaction(
            transaction_id=transaction_id,
            id_tag_info={"status": "Accepted"}
        )

    @on("StatusNotification")
    async def on_status_notification(self, connector_id, error_code, status, timestamp, **kwargs):
        key = f"{self.id}-{connector_id}"
        connector_status[key] = {
            "cpId": self.id,
            "connectorId": connector_id,
            "status": status,
            "errorCode": error_code,
            "timestamp": timestamp
        }

        emoji = {
            "Available": "🟢",
            "Preparing": "🟡",
            "Charging": "⚡",
            "Finishing": "🔌",
            "SuspendedEV": "⏸️",
            "SuspendedEVSE": "⛔",
            "Unavailable": "🚫",
            "Faulted": "❌"
        }.get(status, "📡")

        logging.info(f"{emoji} StatusNotification dari {self.id} | connector={connector_id}, status={status}, error={error_code}")
        return call_result.StatusNotification()

    @on("MeterValues")
    async def on_meter_values(self, connector_id, meter_value=None, transaction_id=None, **kwargs):
        """
        Terima MeterValues dari CP.
        Struktur umum:
        {
          "connectorId": 1,
          "meterValue": [
            {
              "timestamp": "...",
              "sampledValue": [
                {"value": "2", "measurand": "..."}
              ]
            }
          ],
          "transactionId": 12345
        }
        """
        try:
            # beberapa implementasi memakai key 'meter_value' atau 'meterValue' tergantung versi parsing,
            # tapi ocpp routing akan mengirim arg name sesuai spec -> kami gunakan meter_value param name fallback.
            mv = meter_value or []
            if not mv:
                logging.warning(f"⚠️ MeterValues kosong dari {self.id}")
                return call_result.MeterValues()

            # ambil sampledValue pertama yang valid
            sampled = None
            for entry in mv:
                sv_list = entry.get("sampledValue") or entry.get("sampled_value") or entry.get("sampledvalue")
                if sv_list and len(sv_list) > 0:
                    sampled = sv_list[0]
                    break

            if sampled is None:
                raise ValueError("sampledValue not found")

            raw_value = sampled.get("value") or sampled.get("Value") or sampled.get("VAL")
            value = float(raw_value)
            measurand = sampled.get("measurand", "Energy.Active.Import.Register")
            ts = mv[0].get("timestamp") if mv and isinstance(mv, list) else datetime.now(timezone.utc).isoformat()

            logging.info(f"📈 MeterValues diterima dari {self.id} | tx={transaction_id}, connector={connector_id}, value={value} Wh, measurand={measurand}")

            # update transaction jika ada
            if transaction_id:
                tx_id = int(transaction_id)
                tx = transaction_data.get(tx_id)
                if tx:
                    tx["energy"] = tx.get("energy", 0) + value
                    tx["last_update"] = ts
                else:
                    # terima MeterValues untuk tx yang belum ada -> buat minimal entry
                    transaction_data[tx_id] = {
                        "cpId": self.id,
                        "idTag": "Unknown",
                        "connectorId": connector_id,
                        "start_time": ts,
                        "meter_start": 0,
                        "energy": value,
                        "last_update": ts
                    }
            else:
                logging.warning(f"⚠️ MeterValues tanpa transactionId dari {self.id}")

        except Exception as e:
            logging.warning(f"⚠️ Gagal parsing MeterValues dari {self.id}: {e}")

        return call_result.MeterValues()

    @on("StopTransaction")
    async def on_stop_transaction(self, transaction_id, id_tag=None, meter_stop=None, timestamp=None, reason=None, **kwargs):
        try:
            tx_id = int(transaction_id)
        except Exception:
            logging.warning(f"⚠️ StopTransaction diterima dengan transaction_id invalid: {transaction_id}")
            return call_result.StopTransaction()

        tx_info = transaction_data.pop(tx_id, None)
        total_energy = tx_info.get("energy", 0) if tx_info else 0

        tx_record = {
            "transactionId": tx_id,
            "cpId": self.id,
            "idTag": id_tag,
            "start": tx_info.get("start_time") if tx_info else None,
            "stop": timestamp,
            "energy": total_energy,
            "meterStop": meter_stop,
            "reason": reason
        }

        logging.info(
            f"🛑 StopTransaction diterima dari {self.id} | tx={tx_id}, meterStop={meter_stop}, total_energy={total_energy:.2f} Wh"
        )

        transaction_history.append(tx_record)

        # Push data ke API lokal (non-blocking)
        asyncio.create_task(self.push_transaction_to_api(tx_record))

        return call_result.StopTransaction(id_tag_info={"status": "Accepted"})

    async def push_transaction_to_api(self, tx_record):
        """Push hasil transaksi ke endpoint lokal"""
        url = "http://127.0.0.1:8000/api/transaction-history"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=tx_record, timeout=10) as resp:
                    if resp.status in (200, 201):
                        logging.info(f"📤 Data transaksi dikirim ke {url} ✅")
                    else:
                        logging.warning(f"⚠️ Gagal kirim transaksi ke {url}, status={resp.status}")
        except Exception as e:
            logging.error(f"❌ Error push transaction: {e}")

    @on("RemoteStopTransaction")
    async def on_remote_stop_transaction(self, transaction_id, **kwargs):
        logging.info(f"🛑 RemoteStopTransaction diterima dari CSMS | tx={transaction_id}")

        # Balas Accepted
        response = call_result.RemoteStopTransaction(status="Accepted")
        logging.info(f"✅ RemoteStopTransaction.conf dikirim ke CSMS | Status=Accepted")

        # Simulasikan hentikan transaksi
        await asyncio.sleep(2)
        timestamp = datetime.now(timezone.utc).isoformat()
        await self.send_stop_transaction(transaction_id, timestamp)
        return response

    async def send_stop_transaction(self, transaction_id, timestamp):
        """Kirim StopTransaction ke CSMS"""
        req = call.StopTransaction(
            transaction_id=transaction_id,
            id_tag="RFID-001",
            meter_stop=100,
            timestamp=timestamp,
            reason="Remote"
        )
        logging.info(f"📤 StopTransaction.req dikirim ke CSMS | tx={transaction_id}")
        resp = await self.call(req)
        logging.info(f"StopTransaction.conf diterima dari CSMS: {resp}")

# WebSocket handler
async def on_connect(websocket):
    path = websocket.request.path if hasattr(websocket, "request") else "/"
    charge_point_id = path.strip("/") or "unknown_cp"

    logging.info(f"🔗 Koneksi baru dari {charge_point_id}")

    charge_point = ChargePoint(charge_point_id, websocket)
    connected_cps[charge_point_id] = charge_point

    logging.info(f"📋 Handler terdaftar untuk {charge_point_id}: {list(charge_point.route_map.keys())}")
    await charge_point.start()

# HTTP endpoints
async def handle_authorize(request):
    data = await request.json()
    cp_id = data.get("cpId")
    id_tag = data.get("idTag")

    cp = connected_cps.get(cp_id)
    if cp is None:
        return web.json_response({"error": f"Charge Point '{cp_id}' tidak terkoneksi"}, status=404)

    try:
        logging.info(f"📨 Mengirim Authorize.req ke {cp_id} | idTag={id_tag}")
        req = call.Authorize(id_tag=id_tag)
        response = await cp.call(req)

        id_info = getattr(response, "id_tag_info", {})
        status = id_info.get("status", "Unknown") if isinstance(id_info, dict) else getattr(id_info, "status", "Unknown")
        expiry = id_info.get("expiryDate", "N/A") if isinstance(id_info, dict) else getattr(id_info, "expiry_date", "N/A")

        logging.info(f"✅ Authorize.conf diterima dari {cp_id} | Status={status}, Expiry={expiry}")
        return web.json_response({
            "status": status,
            "expiryDate": expiry,
            "cpId": cp_id,
            "idTag": id_tag
        })

    except Exception as e:
        logging.exception("Authorize gagal")
        return web.json_response({"error": str(e)}, status=500)

async def handle_remote_start(request):
    data = await request.json()
    cp_id = data.get("cpId")
    id_tag = data.get("idTag")

    cp = connected_cps.get(cp_id)
    if cp is None:
        return web.json_response({"error": f"Charge Point '{cp_id}' tidak terkoneksi"}, status=404)

    try:
        logging.info(f"🚀 Mengirim RemoteStartTransaction.req ke {cp_id} | idTag={id_tag}")
        req = call.RemoteStartTransaction(id_tag=id_tag)
        response = await cp.call(req)
        status = getattr(response, "status", "Unknown")
        logging.info(f"✅ RemoteStartTransaction.conf dari {cp_id} | Status={status}")

        # jalankan simulasi status sequence pada CSMS (tidak wajib jika CP sendiri mengirim StatusNotification)
        asyncio.create_task(simulate_status_sequence(cp_id))

        return web.json_response({
            "status": status,
            "cpId": cp_id,
            "idTag": id_tag
        })

    except Exception as e:
        logging.exception("RemoteStartTransaction gagal")
        return web.json_response({"error": str(e)}, status=500)

async def simulate_status_sequence(cp_id):
    """Opsional: CSMS-side simulation (biasanya CP kirim sendiri)."""
    cp = connected_cps.get(cp_id)
    if cp is None:
        logging.warning(f"⚠️ Tidak bisa simulasi status, {cp_id} belum terkoneksi.")
        return

    connector_id = 1
    sequence = ["Preparing", "Charging", "Finishing", "Available"]

    for status in sequence:
        await asyncio.sleep(3)
        timestamp = datetime.now(timezone.utc).isoformat()
        logging.info(f"📡 Simulasi StatusNotification dari {cp_id} | status={status}")
        await cp.on_status_notification(
            connector_id=connector_id,
            error_code="NoError",
            status=status,
            timestamp=timestamp
        )

async def handle_status(request):
    return web.json_response({
        "connectors": list(connector_status.values()),
        "transactions": transaction_data,
        "history": transaction_history
    })

async def handle_transactions(request):
    """Menampilkan transaksi aktif (safe JSON)"""
    try:
        safe_data = {
            str(tx_id): {
                k: (v.isoformat() if isinstance(v, datetime) else v)
                for k, v in tx.items()
            }
            for tx_id, tx in transaction_data.items()
        }
        return web.json_response({"transactions": safe_data})
    except Exception as e:
        logging.exception("⚠️ Gagal handle /api/transactions")
        return web.json_response({"error": str(e)}, status=500)

async def handle_transaction_history(request):
    data = await request.json()
    tx_id = data.get("transactionId")

    # cek apakah sudah ada transaksi dengan ID yang sama
    existing = next((tx for tx in transaction_history if tx.get("transactionId") == tx_id), None)

    if existing:
        logging.warning(f"⚠️ Duplikat transaksi di /api/transaction-history diabaikan | tx={tx_id}")
    else:
        transaction_history.append(data)
        logging.info(f"📦 Data transaksi baru disimpan di /api/transaction-history | tx={tx_id}")

    return web.json_response({"status": "received", "duplicate": bool(existing)})

async def handle_remote_stop(request):
    """Menghentikan transaksi aktif dari CSMS"""
    data = await request.json()
    cp_id = data.get("cpId")
    transaction_id = data.get("transactionId")

    cp = connected_cps.get(cp_id)
    if cp is None:
        return web.json_response({"error": f"Charge Point '{cp_id}' tidak terkoneksi"}, status=404)

    try:
        logging.info(f"🛑 Mengirim RemoteStopTransaction.req ke {cp_id} | transactionId={transaction_id}")
        req = call.RemoteStopTransaction(transaction_id=transaction_id)
        response = await cp.call(req)
        status = getattr(response, "status", "Unknown")
        logging.info(f"✅ RemoteStopTransaction.conf dari {cp_id} | Status={status}")

        return web.json_response({
            "status": status,
            "cpId": cp_id,
            "transactionId": transaction_id
        })

    except Exception as e:
        logging.exception("RemoteStopTransaction gagal")
        return web.json_response({"error": str(e)}, status=500)

async def start_websocket_server():
    async with websockets.serve(on_connect, "0.0.0.0", 9000, subprotocols=["ocpp1.6"]):
        logging.info("✅ CSMS WebSocket listening on ws://0.0.0.0:9000")
        await asyncio.Future()

async def start_http_server():
    app = web.Application()
    app.router.add_post("/api/authorize", handle_authorize)
    app.router.add_post("/api/remote-start", handle_remote_start)
    app.router.add_post("/api/transaction-history", handle_transaction_history)
    app.router.add_get("/api/status", handle_status)
    app.router.add_get("/api/transactions", handle_transactions)
    app.router.add_post("/api/remote-stop", handle_remote_stop)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8000)
    await site.start()
    logging.info("🌐 HTTP API listening on http://0.0.0.0:8000")

async def main():
    await asyncio.gather(start_websocket_server(), start_http_server())


if __name__ == "__main__":
    asyncio.run(main())

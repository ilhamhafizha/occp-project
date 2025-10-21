import asyncio
import logging
import websockets
from datetime import datetime, timezone, timedelta
from aiohttp import web
from ocpp.v16 import ChargePoint as BaseChargePoint
from ocpp.v16 import call_result, call
from ocpp.routing import on
from ocpp.v16.enums import RegistrationStatus

logging.basicConfig(level=logging.INFO)
connected_cps = {}  # ✅ Simpan CP yang terkoneksi


# ==========================================================
# 💡 Definisi Class ChargePoint (Handler Event OCPP 1.6)
# ==========================================================
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
            id_tag_info={
                "status": "Accepted",
                "expiryDate": expiry.isoformat(),
                "parentIdTag": None
            }
        )

    @on("StartTransaction")
    async def on_start_transaction(self, connector_id, id_tag, timestamp, meter_start, **kwargs):
        """
        ✅ Handler StartTransaction sesuai OCPP 1.6
        (tanpa reservation_id agar kompatibel dengan semua implementasi CP)
        """
        logging.info(f"⚡ StartTransaction.req diterima dari {self.id} | idTag={id_tag}, connectorId={connector_id}, meterStart={meter_start}")
        return call_result.StartTransaction(
            transaction_id=12345,
            id_tag_info={"status": "Accepted"}
        )


# ==========================================================
# 🔌 WebSocket Connection Handler
# ==========================================================
async def on_connect(websocket):
    path = websocket.request.path if hasattr(websocket, "request") else "/"
    charge_point_id = path.strip("/") or "unknown_cp"

    logging.info(f"🔗 Koneksi baru dari {charge_point_id}")

    charge_point = ChargePoint(charge_point_id, websocket)
    connected_cps[charge_point_id] = charge_point

    # 🔍 Debug: pastikan semua handler terdaftar
    logging.info(f"📋 Handler terdaftar untuk {charge_point_id}: {list(charge_point.route_map.keys())}")

    await charge_point.start()


# ==========================================================
# 📡 HTTP API - /api/authorize
# ==========================================================
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
        if isinstance(id_info, dict):
            status = id_info.get("status", "Unknown")
            expiry = id_info.get("expiryDate") or id_info.get("expiry_date") or "N/A"
        else:
            status = getattr(id_info, "status", "Unknown")
            expiry = getattr(id_info, "expiry_date", None) or getattr(id_info, "expiryDate", "N/A")

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


# ==========================================================
# ⚡ HTTP API - /api/remote-start
# ==========================================================
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

        return web.json_response({
            "status": status,
            "cpId": cp_id,
            "idTag": id_tag
        })

    except Exception as e:
        logging.exception("RemoteStartTransaction gagal")
        return web.json_response({"error": str(e)}, status=500)


# ==========================================================
# 🌐 Jalankan Server WebSocket dan HTTP
# ==========================================================
async def start_websocket_server():
    async with websockets.serve(on_connect, "0.0.0.0", 9000, subprotocols=["ocpp1.6"]):
        logging.info("✅ CSMS WebSocket listening on ws://0.0.0.0:9000")
        await asyncio.Future()  # Biarkan server jalan terus


async def start_http_server():
    app = web.Application()
    app.router.add_post("/api/authorize", handle_authorize)
    app.router.add_post("/api/remote-start", handle_remote_start)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8000)
    await site.start()
    logging.info("🌐 HTTP API listening on http://0.0.0.0:8000")


async def main():
    await asyncio.gather(start_websocket_server(), start_http_server())


if __name__ == "__main__":
    asyncio.run(main())

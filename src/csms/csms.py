import asyncio
import logging
import websockets
from datetime import datetime, timezone
from aiohttp import web
from ocpp.v16 import ChargePoint as BaseChargePoint
from ocpp.v16 import call_result, call
from ocpp.routing import on
from ocpp.v16.enums import RegistrationStatus

logging.basicConfig(level=logging.INFO)
connected_cps = {}  # ✅ Simpan CP yang terkoneksi


class ChargePoint(BaseChargePoint):
    @on("BootNotification")
    async def on_boot_notification(self, charge_point_vendor, charge_point_model, **kwargs):
        logging.info(f"BootNotification received from {self.id} | Vendor: {charge_point_vendor}, Model: {charge_point_model}")
        return call_result.BootNotification(
            current_time=datetime.now(timezone.utc).isoformat(),
            interval=10,
            status=RegistrationStatus.accepted
        )

    @on("Heartbeat")
    async def on_heartbeat(self):
        logging.info(f"💓 Heartbeat received from {self.id}")
        return call_result.Heartbeat(current_time=datetime.now(timezone.utc).isoformat())

    @on("Authorize")
    async def on_authorize(self, id_tag):
        logging.info(f"🔑 Authorize.req received from {self.id} | idTag={id_tag}")
        return call_result.Authorize(
            id_tag_info={
                "status": "Accepted",
                "expiryDate": datetime.now(timezone.utc).isoformat(),
                "parentIdTag": None
            }
        )


async def on_connect(websocket):
    path = websocket.request.path if hasattr(websocket, "request") else "/"
    charge_point_id = path.strip("/") or "unknown_cp"

    logging.info(f"🔌 New connection established from {charge_point_id}")

    charge_point = ChargePoint(charge_point_id, websocket)
    connected_cps[charge_point_id] = charge_point
    await charge_point.start()


# ==========================================================
# 📡 HTTP API - Send Authorize.req
# ==========================================================
async def handle_authorize(request):
    """Terima request HTTP dari Postman dan kirim Authorize.req ke CP"""
    data = await request.json()
    cp_id = data.get("cpId")
    id_tag = data.get("idTag")

    cp = connected_cps.get(cp_id)
    if cp is None:
        return web.json_response({"error": f"Charge Point '{cp_id}' not connected"}, status=404)

    try:
        logging.info(f"📡 Mengirim Authorize.req ke {cp_id} untuk idTag={id_tag}")
        req = call.Authorize(id_tag=id_tag)
        response = await cp.call(req)

        # ✅ Handler tambahan agar log CSMS lebih informatif
        if hasattr(response, "id_tag_info"):
            id_info = response.id_tag_info

            # Kadang id_tag_info adalah object (Bukan dict)
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
        else:
            logging.warning("⚠️ Response dari CP tidak memiliki idTagInfo")
            return web.json_response({"error": "Response tidak valid dari CP"}, status=500)

    except Exception as e:
        logging.exception("Authorize gagal")
        return web.json_response({"error": str(e)}, status=500)


# ==========================================================
# 🚀 Server Setup
# ==========================================================
async def start_websocket_server():
    async with websockets.serve(on_connect, "0.0.0.0", 9000, subprotocols=["ocpp1.6"]):
        logging.info("✅ CSMS WebSocket listening on ws://0.0.0.0:9000")
        await asyncio.Future()


async def start_http_server():
    app = web.Application()
    app.router.add_post("/api/authorize", handle_authorize)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8000)
    await site.start()
    logging.info("🌐 HTTP API listening on http://0.0.0.0:8000")


async def main():
    await asyncio.gather(start_websocket_server(), start_http_server())


if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import logging
import websockets
from datetime import datetime, timezone
from ocpp.v16 import ChargePoint as BaseChargePoint
from ocpp.v16 import call_result
from ocpp.routing import on
from ocpp.v16.enums import RegistrationStatus

logging.basicConfig(level=logging.INFO)


class ChargePoint(BaseChargePoint):
    """Handler untuk tiap koneksi Charge Point."""

    @on('BootNotification')
    async def on_boot_notification(self, charge_point_vendor, charge_point_model, **kwargs):
        logging.info(
            f"BootNotification received from {self.id} | Vendor: {charge_point_vendor}, Model: {charge_point_model}"
        )
        return call_result.BootNotification(
            current_time=datetime.now(timezone.utc).isoformat(),
            interval=10,
            status=RegistrationStatus.accepted
        )

    @on('Heartbeat')
    async def on_heartbeat(self):
        """Menangani pesan Heartbeat dari CP."""
        logging.info(f"💓 Heartbeat received from {self.id}")
        return call_result.Heartbeat(current_time=datetime.now(timezone.utc).isoformat())


async def on_connect(websocket):
    """Handle koneksi baru dari Charge Point."""
    path = websocket.request.path if hasattr(websocket, "request") else "/"
    charge_point_id = path.strip("/") or "unknown_cp"

    logging.info(f"🔌 New connection established from {charge_point_id}")

    charge_point = ChargePoint(charge_point_id, websocket)
    await charge_point.start()


async def main():
    async with websockets.serve(
        on_connect,
        "0.0.0.0",
        9000,
        subprotocols=["ocpp1.6"]
    ):
        logging.info("✅ CSMS is listening on ws://0.0.0.0:9000")
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
p
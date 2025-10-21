import asyncio
import logging
import websockets
from datetime import datetime, timezone
from ocpp.v16 import ChargePoint as BaseChargePoint
from ocpp.v16 import call, call_result
from ocpp.routing import on
from ocpp.v16.enums import RegistrationStatus

logging.basicConfig(level=logging.INFO)


class ChargePoint(BaseChargePoint):
    async def send_boot_notification(self):
        req = call.BootNotification(
            charge_point_model="DemoModel",
            charge_point_vendor="DemoVendor"
        )
        logging.info("BootNotification sent")
        resp = await self.call(req)
        logging.info(f"BootNotification.conf: {resp}")

        if getattr(resp, "status", None) == RegistrationStatus.accepted:
            interval = getattr(resp, "interval", 10)
            logging.info(f"Boot accepted ✅ | heartbeat interval={interval}s")
            asyncio.create_task(self.heartbeat_loop(interval))
        else:
            logging.warning("Boot rejected ❌")

    async def heartbeat_loop(self, interval):
        while True:
            await asyncio.sleep(interval)
            req = call.Heartbeat()
            resp = await self.call(req)
            logging.info(f"Heartbeat.conf: {resp}")

    @on("Authorize")
    async def on_authorize(self, id_tag):
        logging.info(f"🔑 Authorize.req received | idTag={id_tag}")
        return call_result.Authorize(
            id_tag_info={
                "status": "Accepted",
                "expiryDate": datetime.now(timezone.utc).isoformat(),
                "parentIdTag": None
            }
        )

    @on("RemoteStartTransaction")
    async def on_remote_start_transaction(self, id_tag, **kwargs):
        logging.info(f"⚡ RemoteStartTransaction diterima | idTag={id_tag}")

        # Kirim balasan ke CSMS bahwa CP menerima perintah ini
        response = call_result.RemoteStartTransaction(status="Accepted")
        asyncio.create_task(self.start_transaction(id_tag))
        return response

    async def start_transaction(self, id_tag):
        """Simulasi pengiriman StartTransaction.req ke CSMS"""
        await asyncio.sleep(2)  # simulasi delay
        req = call.StartTransaction(
            connector_id=1,
            id_tag=id_tag,
            timestamp=datetime.now(timezone.utc).isoformat(),
            meter_start=0,
            reservation_id=0
        )
        logging.info(f"🔋 Mengirim StartTransaction.req untuk idTag={id_tag}")
        resp = await self.call(req)
        logging.info(f"StartTransaction.conf: {resp}")

async def main():
    cp_id = "CP_1"
    uri = f"ws://127.0.0.1:9000/{cp_id}"

    logging.info(f"Connecting to {uri} ...")
    async with websockets.connect(uri, subprotocols=["ocpp1.6"]) as ws:
        cp = ChargePoint(cp_id, ws)
        asyncio.create_task(cp.start())
        await cp.send_boot_notification()
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())

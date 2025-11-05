# src/cp/cp.py
import asyncio
import logging
import websockets
from datetime import datetime, timezone
from ocpp.v16 import call, call_result
from ocpp.v16 import ChargePoint as BaseChargePoint
from ocpp.routing import on

logging.basicConfig(level=logging.INFO)


class ChargePoint(BaseChargePoint):
    async def send_boot_notification(self):
        req = call.BootNotification(
            charge_point_model="DemoModel",
            charge_point_vendor="DemoVendor"
        )
        logging.info("📡 BootNotification sent")
        resp = await self.call(req)
        logging.info(f"BootNotification.conf: {resp}")

        if getattr(resp, "status", None) == "Accepted" or getattr(resp, "status", None) == "Accepted":
            interval = getattr(resp, "interval", 10)
            logging.info(f"Boot accepted ✅ | heartbeat interval={interval}s")
            asyncio.create_task(self.heartbeat_loop(60))  # kirim heartbeat tiap 60s sesuai requestmu
        else:
            logging.warning("Boot rejected ❌")

    async def heartbeat_loop(self, interval_seconds):
        while True:
            await asyncio.sleep(interval_seconds)
            req = call.Heartbeat()
            try:
                resp = await self.call(req)
                logging.info(f"Heartbeat.conf: {resp}")
            except Exception as e:
                logging.warning(f"Heartbeat failed: {e}")

    @on("Authorize")
    async def on_authorize(self, id_tag):
        logging.info(f"🔑 Authorize.req received | idTag={id_tag}")
        expiry = datetime.now(timezone.utc).isoformat()
        return call_result.Authorize(
            id_tag_info={
                "status": "Accepted",
                "expiryDate": expiry,
                "parentIdTag": None
            }
        )

    @on("RemoteStartTransaction")
    async def on_remote_start_transaction(self, id_tag, **kwargs):
        logging.info(f"⚡ RemoteStartTransaction diterima | idTag={id_tag}")

        # Kirim accepted segera
        response = call_result.RemoteStartTransaction(status="Accepted")
        # jalankan proses start_transaction sebagai task agar tidak block
        asyncio.create_task(self.start_transaction_flow(id_tag))
        return response

    async def start_transaction_flow(self, id_tag):
        # Status Preparing
        await self.send_status("Preparing")
        await asyncio.sleep(2)

        # Kirim StartTransaction ke CSMS, tunggu conf -> CSMS akan menentukan transactionId
        now = datetime.now(timezone.utc).isoformat()
        req_start = call.StartTransaction(
            connector_id=1,
            id_tag=id_tag,
            timestamp=now,
            meter_start=0,
            reservation_id=0
        )
        logging.info("🔋 Mengirim StartTransaction.req ke CSMS")
        resp = await self.call(req_start)
        # ambil transaction id dari response
        tx_id = getattr(resp, "transaction_id", None)
        logging.info(f"StartTransaction.conf: {resp} -> tx_id={tx_id}")

        # Setelah accepted, kirim Charging status
        await self.send_status("Charging")

        # Simulasi kirim MeterValues tiap 10 detik selama 1 menit (6 kali)
        energy = 0
        for i in range(6):
            await asyncio.sleep(10)
            energy += 2  # naik 2 Wh tiap iter
            now = datetime.now(timezone.utc).isoformat()
            logging.info(f"📈 Sending MeterValues: {energy} Wh (tx={tx_id})")
            mv_req = call.MeterValues(
                connector_id=1,
                meter_value=[
                    {
                        "timestamp": now,
                        "sampledValue": [
                            {"value": str(energy), "measurand": "Energy.Active.Import.Register"}
                        ]
                    }
                ],
                transaction_id=tx_id
            )
            try:
                resp = await self.call(mv_req)
                logging.debug(f"MeterValues.conf: {resp}")
            except Exception as e:
                logging.warning(f"MeterValues call failed: {e}")

        # Simulasi selesai -> Finishing + StopTransaction
        await self.send_status("Finishing")
        await asyncio.sleep(2)

        stop_ts = datetime.now(timezone.utc).isoformat()
        stop_req = call.StopTransaction(
            transaction_id=tx_id,
            id_tag=id_tag,
            meter_stop=energy,
            timestamp=stop_ts,
            reason="Local"
        )
        logging.info(f"🛑 Sending StopTransaction req tx={tx_id}, energy={energy}")
        try:
            resp = await self.call(stop_req)
            logging.info(f"StopTransaction.conf: {resp}")
        except Exception as e:
            logging.warning(f"StopTransaction call failed: {e}")

        await asyncio.sleep(1)
        await self.send_status("Available")

    async def send_status(self, status):
        now = datetime.now(timezone.utc).isoformat()
        req = call.StatusNotification(
            connector_id=1,
            error_code="NoError",
            status=status,
            timestamp=now
        )
        try:
            await self.call(req)
        except Exception as e:
            logging.warning(f"StatusNotification failed: {e}")


async def main():
    uri = "ws://127.0.0.1:9000/CP_1"
    logging.info(f"🔗 Connecting to {uri} ...")
    async with websockets.connect(uri, subprotocols=["ocpp1.6"]) as ws:
        cp = ChargePoint("CP_1", ws)
        asyncio.create_task(cp.send_boot_notification())
        await cp.start()


if __name__ == "__main__":
    asyncio.run(main())

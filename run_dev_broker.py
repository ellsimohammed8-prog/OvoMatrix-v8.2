"""
OvoMatrix v8.0 — Minimal Pure-Python MQTT Broker
==================================================
A lightweight, dependency-free MQTT 3.1.1 broker for local development
and UI testing. Uses only Python standard library (asyncio + socket).

Supports:
  - CONNECT / CONNACK
  - SUBSCRIBE / SUBACK
  - PUBLISH (QoS 0 and 1) with fan-out to all matching subscribers
  - PUBACK (QoS 1)
  - PINGREQ / PINGRESP
  - DISCONNECT

NOT for production use. Anonymous-only, no TLS, no persistence.

Usage:
    python run_dev_broker.py
    python run_dev_broker.py --host 127.0.0.1 --port 1883
"""

import asyncio
import argparse
import logging
import signal
import struct
import sys
from typing import Dict, List, Set, Tuple

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  \033[92m[OvoBroker]\033[0m  %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("ovobroker")


# ── MQTT Packet Types ─────────────────────────────────────────────────────────
CONNECT    = 0x10
CONNACK    = 0x20
PUBLISH    = 0x30
PUBACK     = 0x40
SUBSCRIBE  = 0x80
SUBACK     = 0x90
PINGREQ    = 0xC0
PINGRESP   = 0xD0
DISCONNECT = 0xE0


def _encode_remaining(length: int) -> bytes:
    """Encode remaining length using MQTT variable-length encoding."""
    out = []
    while True:
        byte = length % 128
        length //= 128
        if length > 0:
            byte |= 0x80
        out.append(byte)
        if length == 0:
            break
    return bytes(out)


def _decode_remaining(data: bytes, offset: int) -> Tuple[int, int]:
    """Decode MQTT variable-length remaining length. Returns (value, new_offset)."""
    multiplier = 1
    value = 0
    while True:
        byte = data[offset]
        offset += 1
        value += (byte & 0x7F) * multiplier
        multiplier *= 128
        if not (byte & 0x80):
            break
    return value, offset


def _read_str(data: bytes, offset: int) -> Tuple[str, int]:
    """Read a length-prefixed UTF-8 string. Returns (string, new_offset)."""
    length = struct.unpack_from(">H", data, offset)[0]
    offset += 2
    return data[offset:offset + length].decode("utf-8", errors="replace"), offset + length


# ── Client state ──────────────────────────────────────────────────────────────
class MqttClient:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader   = reader
        self.writer   = writer
        self.client_id: str = ""
        self.subscriptions: Set[str] = set()
        self.addr = writer.get_extra_info("peername", ("?", 0))

    def matches(self, topic: str) -> bool:
        """Return True if any subscription filter matches the given topic."""
        for filt in self.subscriptions:
            if _topic_matches(filt, topic):
                return True
        return False

    async def send(self, data: bytes):
        self.writer.write(data)
        await self.writer.drain()

    def close(self):
        try:
            self.writer.close()
        except Exception:
            pass


def _topic_matches(filt: str, topic: str) -> bool:
    """MQTT topic wildcard matching (# and +)."""
    fp = filt.split("/")
    tp = topic.split("/")
    i = 0
    while i < len(fp):
        if fp[i] == "#":
            return True
        if i >= len(tp):
            return False
        if fp[i] != "+" and fp[i] != tp[i]:
            return False
        i += 1
    return i == len(tp)


# ── Broker ────────────────────────────────────────────────────────────────────
class MqttBroker:
    def __init__(self):
        self.clients: List[MqttClient] = []

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        client = MqttClient(reader, writer)
        self.clients.append(client)
        log.info("New connection from %s:%d", *client.addr)
        try:
            await self._client_loop(client)
        except (asyncio.IncompleteReadError, ConnectionResetError, OSError):
            pass
        finally:
            self.clients.remove(client)
            client.close()
            label = client.client_id or f"{client.addr[0]}:{client.addr[1]}"
            log.info("Client disconnected: %s", label)

    async def _read_packet(self, client: MqttClient) -> Tuple[int, bytes]:
        """Read one MQTT packet; returns (packet_type_byte, payload)."""
        header = await client.reader.readexactly(1)
        ptype = header[0]
        # Read variable-length remaining length
        remaining = 0
        multiplier = 1
        while True:
            b = await client.reader.readexactly(1)
            remaining += (b[0] & 0x7F) * multiplier
            multiplier *= 128
            if not (b[0] & 0x80):
                break
        payload = await client.reader.readexactly(remaining) if remaining else b""
        return ptype, payload

    async def _client_loop(self, client: MqttClient):
        while True:
            ptype, payload = await self._read_packet(client)
            ptype_id = ptype & 0xF0

            if ptype_id == CONNECT:
                await self._handle_connect(client, payload)

            elif ptype_id == PUBLISH:
                await self._handle_publish(client, ptype, payload)

            elif ptype_id == SUBSCRIBE:
                await self._handle_subscribe(client, payload)

            elif ptype_id == PINGREQ:
                await client.send(bytes([PINGRESP, 0x00]))

            elif ptype_id == DISCONNECT:
                break

    async def _handle_connect(self, client: MqttClient, payload: bytes):
        offset = 0
        # Skip protocol name + level + flags
        proto_name, offset = _read_str(payload, offset)
        proto_level  = payload[offset]; offset += 1
        connect_flags= payload[offset]; offset += 1
        keep_alive   = struct.unpack_from(">H", payload, offset)[0]; offset += 2
        # Read client_id
        client.client_id, offset = _read_str(payload, offset)
        # CONNACK: session-present=0, return-code=0 (accepted)
        await client.send(bytes([CONNACK, 0x02, 0x00, 0x00]))
        log.info("CONNECT accepted: client_id='%s'", client.client_id)

    async def _handle_publish(self, client: MqttClient, ptype: int, payload: bytes):
        qos    = (ptype & 0x06) >> 1
        offset = 0
        topic, offset = _read_str(payload, offset)
        packet_id = None
        if qos > 0:
            packet_id = struct.unpack_from(">H", payload, offset)[0]
            offset += 2
        message = payload[offset:]

        log.debug("PUBLISH topic=%s len=%d qos=%d", topic, len(message), qos)

        # Fan-out to all other matching subscribers
        await self._fanout(client, topic, message, qos)

        # Send PUBACK for QoS 1
        if qos == 1 and packet_id is not None:
            await client.send(bytes([PUBACK, 0x02]) + struct.pack(">H", packet_id))

    async def _fanout(self, sender: MqttClient, topic: str, message: bytes, qos: int):
        """Deliver message to all subscribers whose filters match the topic."""
        targets = [c for c in self.clients if c is not sender and c.matches(topic)]
        for target in targets:
            try:
                topic_enc = topic.encode("utf-8")
                # Build PUBLISH packet (QoS 0 to subscribers for simplicity)
                fixed    = bytes([PUBLISH])
                var      = struct.pack(">H", len(topic_enc)) + topic_enc + message
                packet   = fixed + _encode_remaining(len(var)) + var
                await target.send(packet)
            except Exception as e:
                log.warning("Failed to deliver to %s: %s", target.client_id, e)

    async def _handle_subscribe(self, client: MqttClient, payload: bytes):
        offset = 0
        packet_id = struct.unpack_from(">H", payload, offset)[0]; offset += 2
        return_codes = []
        while offset < len(payload):
            filt, offset = _read_str(payload, offset)
            qos = payload[offset]; offset += 1
            client.subscriptions.add(filt)
            return_codes.append(min(qos, 1))  # grant up to QoS 1
            log.info("SUBSCRIBE client='%s' filter='%s' qos=%d", client.client_id, filt, qos)
        suback = bytes([SUBACK]) + _encode_remaining(2 + len(return_codes)) + struct.pack(">H", packet_id) + bytes(return_codes)
        await client.send(suback)


# ── Entry point ───────────────────────────────────────────────────────────────
async def main(host: str, port: int):
    broker  = MqttBroker()
    server  = await asyncio.start_server(broker.handle_client, host, port)

    log.info("\033[1;92m══════════════════════════════════════════\033[0m")
    log.info("\033[1;92m  OvoMatrix Dev MQTT Broker — READY        \033[0m")
    log.info("\033[1;92m  Listening on  %s:%d                      \033[0m", host, port)
    log.info("\033[1;92m  Mode: Anonymous  |  QoS 0 & 1            \033[0m")
    log.info("\033[1;92m══════════════════════════════════════════\033[0m")
    log.info("Press Ctrl+C to stop.")

    stop = asyncio.Event()

    def _shutdown():
        log.info("Shutdown signal received.")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            pass  # Windows doesn't support add_signal_handler for all signals

    async with server:
        await stop.wait()

    log.info("\033[92mBroker stopped cleanly.\033[0m")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OvoMatrix Dev MQTT Broker")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=1883, help="Bind port (default: 1883)")
    args = parser.parse_args()

    try:
        asyncio.run(main(args.host, args.port))
    except KeyboardInterrupt:
        print("\n\033[92m[OvoBroker] Stopped.\033[0m")

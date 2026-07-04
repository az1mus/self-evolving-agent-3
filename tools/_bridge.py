# _bridge.py — SEA CLI Python 节点桥接协议辅助库
#
# 每个 Python 工具节点通过 stdin/stdout JSON Lines 协议与 Rust Runtime 通信。
#
# 协议信封 (BridgeMessage):
#   {"trace_id": "...", "in_response_to": "..." | null, "payload": {...}, "content_type": "..."}
#
# 用法:
#   from _bridge import Bridge, BridgeMessage
#   bridge = Bridge()
#   for msg in bridge.iter_messages():
#       reply = handle(msg)
#       bridge.send(reply)

import sys
import json
from typing import Any, Iterator


class BridgeMessage:
    """从 Rust Runtime 发来的桥接消息。"""

    def __init__(self, raw: dict):
        self.trace_id: str = raw.get("trace_id", "")
        self.in_response_to: str | None = raw.get("in_response_to")
        self.payload: dict = raw.get("payload", {})
        self.content_type: str = raw.get("content_type", "application/json")

    def get(self, key: str, default: Any = None) -> Any:
        return self.payload.get(key, default)


class Bridge:
    """stdin/stdout JSON Lines 协议桥接。

    自动处理编码/解码，提供消息迭代器。
    """

    def __init__(self):
        self._stdin = sys.stdin
        self._stdout = sys.stdout

    def iter_messages(self) -> Iterator[BridgeMessage]:
        """阻塞式迭代：从 stdin 逐行读取，yield BridgeMessage。"""
        for line in self._stdin:
            line = line.strip()
            if not line:
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as e:
                self._log_error(f"json_parse_error: {e}")
                continue
            yield BridgeMessage(raw)

    def send(self, trace_id: str, payload: dict,
             in_response_to: str | None = None,
             content_type: str = "application/json",
             status: str = "ok") -> None:
        """发送一条回复消息到 stdout。"""
        response = {
            "trace_id": trace_id,
            "in_response_to": in_response_to,
            "payload": {"status": status, **payload},
            "content_type": content_type,
        }
        line = json.dumps(response, ensure_ascii=False, default=str)
        self._stdout.write(line + "\n")
        self._stdout.flush()

    def send_error(self, trace_id: str, error: str,
                   in_response_to: str | None = None) -> None:
        """发送错误回复。"""
        self.send(
            trace_id=trace_id,
            payload={"error": error},
            in_response_to=in_response_to,
            status="error",
        )

    @staticmethod
    def _log_error(msg: str) -> None:
        print(json.dumps({
            "trace_id": "bridge",
            "payload": {"status": "error", "error": msg},
            "content_type": "application/json",
        }), flush=True)


# 便捷入口
def main_loop(handler):
    """一行启动标准消息循环。

    handler 签名: handler(msg: BridgeMessage) -> dict
    返回的 dict 作为 payload 发回 Runtime，status 自动置为 "ok"。
    """
    bridge = Bridge()
    for msg in bridge.iter_messages():
        try:
            result = handler(msg)
            bridge.send(
                trace_id=msg.trace_id,
                payload=result,
                in_response_to=msg.in_response_to,
            )
        except Exception as e:
            bridge.send_error(
                trace_id=msg.trace_id,
                error=str(e),
                in_response_to=msg.in_response_to,
            )

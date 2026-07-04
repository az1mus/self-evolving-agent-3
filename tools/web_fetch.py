# web_fetch.py — HTTP 请求节点
#
# 发送 HTTP GET 请求，返回响应内容和元数据。
# 支持超时限制和响应大小截断。
#
# 输入格式 (payload):
#   {"action": "fetch", "url": "https://example.com", "timeout_ms": 15000, "headers": {"Accept": "text/plain"}}
#
# 输出格式:
#   {"status": "ok", "url": "...", "status_code": 200, "content_type": "...", "content": "...", "size": 1234, "duration_ms": 456}

import os
import sys
import time
import urllib.request
import urllib.error
import ssl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _bridge import Bridge, BridgeMessage


class WebFetchNode:
    """HTTP 请求节点——发送 GET 请求并返回响应内容。"""

    # 最大响应大小（5 MiB）
    MAX_RESPONSE_SIZE = 5 * 1024 * 1024

    # 最大超时（60 秒）
    MAX_TIMEOUT_MS = 60_000

    # 默认超时（15 秒）
    DEFAULT_TIMEOUT_MS = 15_000

    # 允许的协议
    ALLOWED_SCHEMES = {"http", "https"}

    def handle(self, msg: BridgeMessage) -> dict:
        action = msg.get("action", "").lower()
        if action != "fetch":
            return {"error": f"unknown action: {action}"}

        url = msg.get("url", "")
        if not url:
            return {"error": "missing 'url' field"}

        timeout_ms = min(
            int(msg.get("timeout_ms", self.DEFAULT_TIMEOUT_MS)),
            self.MAX_TIMEOUT_MS,
        )

        # 用户自定义 HTTP headers
        user_headers = msg.get("headers", {})
        if not isinstance(user_headers, dict):
            user_headers = {}

        return self._fetch(url, timeout_ms, user_headers)

    def _fetch(self, url: str, timeout_ms: int, headers: dict) -> dict:
        # 协议检查
        from urllib.parse import urlparse
        parsed = urlparse(url)
        if parsed.scheme not in self.ALLOWED_SCHEMES:
            return {"error": f"不允许的协议: {parsed.scheme}"}

        # 安全校验：禁止内网 IP（防止 SSRF）
        if parsed.hostname:
            host = parsed.hostname.lower()
            if self._is_private_host(host):
                return {"error": f"禁止访问内网地址: {host}"}

        timeout_sec = timeout_ms / 1000.0

        start = time.monotonic()

        try:
            # 构造请求
            req = urllib.request.Request(url)

            # 默认 User-Agent
            req.add_header("User-Agent", "SEA-CLI/0.1 (+https://github.com/sea-cli)")

            # 用户自定义 headers（不能覆盖安全相关项）
            for key, value in headers.items():
                if key.lower() not in ("host", "user-agent"):
                    req.add_header(key, str(value))

            # SSL 上下文（允许自签名证书仅用于开发）
            ctx = ssl.create_default_context()
            ctx.check_hostname = True
            ctx.verify_mode = ssl.CERT_REQUIRED

            resp = urllib.request.urlopen(req, timeout=timeout_sec, context=ctx)

            # 读取响应体（有大小限制）
            raw = resp.read(self.MAX_RESPONSE_SIZE + 1)
            if len(raw) > self.MAX_RESPONSE_SIZE:
                return {
                    "url": url,
                    "status_code": resp.status,
                    "content_type": resp.headers.get("Content-Type", "unknown"),
                    "size": len(raw),
                    "error": f"响应太大 (>{self.MAX_RESPONSE_SIZE} bytes)",
                    "duration_ms": int((time.monotonic() - start) * 1000),
                }

            # 自动解码
            charset = self._extract_charset(resp.headers.get("Content-Type", ""))
            try:
                text = raw.decode(charset, errors="replace")
            except (LookupError, UnicodeDecodeError):
                text = raw.decode("utf-8", errors="replace")

            duration_ms = int((time.monotonic() - start) * 1000)

            return {
                "url": url,
                "status_code": resp.status,
                "content_type": resp.headers.get("Content-Type", "unknown"),
                "content": text,
                "size": len(raw),
                "duration_ms": duration_ms,
            }

        except urllib.error.HTTPError as e:
            return {
                "url": url,
                "status_code": e.code,
                "error": str(e),
                "duration_ms": int((time.monotonic() - start) * 1000),
            }
        except urllib.error.URLError as e:
            return {
                "url": url,
                "error": f"连接失败: {e.reason}",
                "duration_ms": int((time.monotonic() - start) * 1000),
            }
        except TimeoutError:
            return {
                "url": url,
                "error": f"请求超时 (>{timeout_ms}ms)",
                "duration_ms": int((time.monotonic() - start) * 1000),
            }
        except Exception as e:
            return {"url": url, "error": f"fetch 失败: {e}"}

    @staticmethod
    def _is_private_host(host: str) -> bool:
        """检查是否为内网/私有地址段。"""
        # IPv4 私有段
        if host.startswith("10.") or host.startswith("192.168."):
            return True
        if host.startswith("172."):
            try:
                second = int(host.split(".")[1])
                if 16 <= second <= 31:
                    return True
            except (ValueError, IndexError):
                pass
        # localhost
        if host in ("localhost", "127.0.0.1", "::1", "0.0.0.0"):
            return True
        return False

    @staticmethod
    def _extract_charset(content_type: str) -> str:
        """从 Content-Type 头提取 charset。"""
        content_type = content_type.lower()
        if "charset=" in content_type:
            parts = content_type.split("charset=")
            if len(parts) > 1:
                return parts[1].split(";")[0].strip()
        return "utf-8"


def main():
    node = WebFetchNode()
    bridge = Bridge()
    for msg in bridge.iter_messages():
        try:
            result = node.handle(msg)
            bridge.send(
                trace_id=msg.trace_id,
                payload=result,
                in_response_to=msg.in_response_to,
                status="error" if "error" in result else "ok",
            )
        except Exception as e:
            bridge.send_error(trace_id=msg.trace_id, error=str(e), in_response_to=msg.in_response_to)


if __name__ == "__main__":
    main()

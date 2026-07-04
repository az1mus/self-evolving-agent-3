# shell_exec.py — Shell 命令执行节点
#
# 沙箱内执行系统命令，支持超时限制和输出截断。
# 适用于临时脚本、系统查询等短任务。
#
# 输入格式 (payload):
#   {"action": "exec", "command": "echo hello", "timeout_ms": 10000, "work_dir": "."}
#
# 输出格式:
#   {"status": "ok", "stdout": "...", "stderr": "...", "exit_code": 0, "duration_ms": 123}

import os
import sys
import subprocess
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _bridge import Bridge, BridgeMessage


class ShellNode:
    """Shell 执行节点——执行短命令并捕获输出。"""

    # 最大输出字节数（1 MiB）
    MAX_OUTPUT_BYTES = 1 * 1024 * 1024

    # 最大超时（120 秒）
    MAX_TIMEOUT_MS = 120_000

    # 默认超时（30 秒）
    DEFAULT_TIMEOUT_MS = 30_000

    # 命令黑名单关键词（防止灾难性操作）
    BLOCKED_KEYWORDS = [
        "rm -rf /",
        "mkfs.",
        "dd if=",
        ":(){ :|:& };:",  # fork bomb
    ]

    def __init__(self):
        self.workspace = os.environ.get("SEA_WORKSPACE_DIR", os.getcwd())

    def handle(self, msg: BridgeMessage) -> dict:
        action = msg.get("action", "").lower()
        if action != "exec":
            return {"error": f"unknown action: {action}"}

        command = msg.get("command", "")
        if not command:
            return {"error": "missing 'command' field"}

        timeout_ms = min(
            int(msg.get("timeout_ms", self.DEFAULT_TIMEOUT_MS)),
            self.MAX_TIMEOUT_MS,
        )
        work_dir = msg.get("work_dir", ".")

        return self._exec(command, timeout_ms, work_dir)

    def _exec(self, command: str, timeout_ms: int, work_dir: str) -> dict:
        # 黑名单检查
        cmd_lower = command.lower().replace(" ", "")
        for blocked in self.BLOCKED_KEYWORDS:
            if blocked.replace(" ", "") in cmd_lower:
                return {"error": f"命令被阻止: {blocked}"}

        start = time.monotonic()

        try:
            # Windows 使用 cmd，Unix 使用 sh
            if sys.platform == "win32":
                proc = subprocess.Popen(
                    ["cmd", "/c", command],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=os.path.join(self.workspace, work_dir),
                    text=True,
                    encoding="utf-8",
                )
            else:
                proc = subprocess.Popen(
                    ["sh", "-c", command],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=os.path.join(self.workspace, work_dir),
                    text=True,
                    encoding="utf-8",
                )

            timeout_sec = timeout_ms / 1000.0
            try:
                stdout, stderr = proc.communicate(timeout=timeout_sec)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, stderr = proc.communicate()
                return {
                    "stdout": self._truncate(stdout or ""),
                    "stderr": self._truncate(stderr or ""),
                    "exit_code": -1,
                    "error": f"命令超时 (>{timeout_ms}ms)",
                    "duration_ms": int((time.monotonic() - start) * 1000),
                }

            duration_ms = int((time.monotonic() - start) * 1000)

            return {
                "stdout": self._truncate(stdout or ""),
                "stderr": self._truncate(stderr or ""),
                "exit_code": proc.returncode,
                "duration_ms": duration_ms,
            }

        except FileNotFoundError:
            return {"error": "shell 不可用"}
        except Exception as e:
            return {"error": f"exec 失败: {e}"}

    def _truncate(self, text: str) -> str:
        if len(text.encode("utf-8", errors="replace")) > self.MAX_OUTPUT_BYTES:
            return text[:self.MAX_OUTPUT_BYTES] + "\n... [输出截断]"
        return text


def main():
    node = ShellNode()
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

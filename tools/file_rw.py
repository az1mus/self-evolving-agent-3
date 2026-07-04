# file_rw.py — 文件读写节点
#
# 限定工作区目录，支持 read/write/list 三项操作。
# 默认工作区为当前工作目录（可通过 SEA_WORKSPACE_DIR 环境变量覆盖）。
#
# 输入格式 (payload):
#   {"action": "read" | "write" | "list", "path": "相对路径", "content": "写入内容 (仅 write)"}
#
# 输出格式:
#   {"status": "ok", "action": "...", "path": "...", "content" | "files" | "error": "..."}

import os
import sys
from pathlib import Path

# 将 tools/ 目录加入路径以导入 _bridge
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _bridge import Bridge, BridgeMessage


class FileNode:
    """文件读写节点——所有路径操作限定在工作区目录内。"""

    # 允许的扩展名白名单（留空不限制）
    ALLOWED_EXTENSIONS = {
        ".txt", ".md", ".py", ".js", ".ts", ".rs", ".toml",
        ".json", ".yaml", ".yml", ".csv", ".html", ".css",
        ".xml", ".sh", ".bat", ".ps1", ".cfg", ".ini", ".log",
    }

    # 最大文件大小 (10 MiB)
    MAX_FILE_SIZE = 10 * 1024 * 1024

    def __init__(self):
        self.workspace = Path(os.environ.get("SEA_WORKSPACE_DIR", os.getcwd())).resolve()
        if not self.workspace.exists():
            self.workspace.mkdir(parents=True, exist_ok=True)

    def handle(self, msg: BridgeMessage) -> dict:
        action = msg.get("action", "").lower()
        path_str = msg.get("path", "")

        if not path_str:
            return {"error": "missing 'path' field"}

        # 解析并校验路径
        resolved = self._resolve_path(path_str)

        match action:
            case "read":
                return self._read(resolved)
            case "write":
                content = str(msg.get("content", ""))
                return self._write(resolved, content)
            case "list":
                return self._list_dir(resolved)
            case _:
                return {"error": f"unknown action: {action}"}

    # ── 路径校验 ──

    def _resolve_path(self, path_str: str) -> Path:
        """将相对路径解析为工作区内的绝对路径，拒绝越权访问。"""
        candidate = (self.workspace / path_str).resolve()

        # 安全检查：路径不得离开工作区
        try:
            candidate.relative_to(self.workspace)
        except ValueError:
            raise PermissionError(f"路径越权: {path_str}")

        return candidate

    # ── 操作 ──

    def _read(self, path: Path) -> dict:
        if not path.is_file():
            return {"error": f"文件不存在: {path.name}"}

        if path.stat().st_size > self.MAX_FILE_SIZE:
            return {"error": f"文件过大 (>{self.MAX_FILE_SIZE} bytes)"}

        content = path.read_text(encoding="utf-8", errors="replace")
        return {
            "action": "read",
            "path": str(path.relative_to(self.workspace)),
            "size": len(content),
            "content": content,
        }

    def _write(self, path: Path, content: str) -> dict:
        # 扩展名校验
        ext = path.suffix.lower()
        if self.ALLOWED_EXTENSIONS and ext not in self.ALLOWED_EXTENSIONS:
            return {"error": f"不允许的文件类型: {ext}"}

        # 大小校验
        content_bytes = content.encode("utf-8")
        if len(content_bytes) > self.MAX_FILE_SIZE:
            return {"error": f"写入内容过大 (>{self.MAX_FILE_SIZE} bytes)"}

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

        return {
            "action": "write",
            "path": str(path.relative_to(self.workspace)),
            "size": len(content_bytes),
        }

    def _list_dir(self, path: Path) -> dict:
        if not path.is_dir():
            return {"error": f"目录不存在: {path.name}"}

        entries = []
        for entry in sorted(path.iterdir()):
            entry_type = "dir" if entry.is_dir() else "file"
            size = entry.stat().st_size if entry.is_file() else 0
            entries.append({
                "name": entry.name,
                "type": entry_type,
                "size": size,
            })

        return {
            "action": "list",
            "path": str(path.relative_to(self.workspace)),
            "count": len(entries),
            "files": entries,
        }


def main():
    node = FileNode()
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
        except PermissionError as e:
            bridge.send_error(trace_id=msg.trace_id, error=str(e), in_response_to=msg.in_response_to)
        except Exception as e:
            bridge.send_error(trace_id=msg.trace_id, error=f"internal: {e}", in_response_to=msg.in_response_to)


if __name__ == "__main__":
    main()

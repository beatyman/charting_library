"""HTTP 服务启动入口：

    python -m quant.server --host 127.0.0.1 --port 3000
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import uvicorn  # noqa: E402

from quant.backtest import localAPI  # noqa: E402
from quant.server.app import create_app  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description="chan-core HTTP K 线数据服务")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=3000)
    p.add_argument("--data-root", default=str(localAPI.DATA_ROOT_DEFAULT))
    p.add_argument("--cache-size", type=int, default=16)
    p.add_argument("--reload", action="store_true", help="开发模式：代码改动自动重载")
    args = p.parse_args()

    if args.reload:
        import os
        os.environ["CHAN_DATA_ROOT"] = str(args.data_root)
        uvicorn.run("quant.server.app:_reload_app", host=args.host, port=args.port,
                    reload=True, factory=True)
    else:
        app = create_app(data_root=args.data_root, cache_size=args.cache_size)
        uvicorn.run(app, host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    sys.exit(main())

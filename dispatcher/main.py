from __future__ import annotations

import asyncio
import logging
from typing import Any

import uvicorn

from .api import create_app

logging.basicConfig(level=logging.INFO)


async def _serve() -> None:
    app = create_app()
    config = uvicorn.Config(app=app, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


def main(argv: list[str] | None = None) -> Any:  # pragma: no cover - CLI entry
    asyncio.run(_serve())


if __name__ == "__main__":  # pragma: no cover - CLI entry
    main()

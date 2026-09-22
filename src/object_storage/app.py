"""FastAPI 应用装配：创建共享的 ObjectStorageService 并挂载路由。"""

from __future__ import annotations

from fastapi import FastAPI

from . import api
from .service import ObjectStorageService
from .store import StoreConfig


def create_app(
    config: StoreConfig | None = None,
    *,
    service: ObjectStorageService | None = None,
) -> FastAPI:
    app = FastAPI(title="local-object-storage", version="1.0.0")
    svc = service or ObjectStorageService(config or StoreConfig())
    api.set_default_service(svc)
    app.state.service = svc
    app.include_router(api.router)

    @app.get("/")
    def root() -> dict:
        return {
            "service": "local-object-storage",
            "docs": "/docs",
            "base": "/storage/v1",
        }

    return app


app = create_app()

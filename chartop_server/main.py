import os
import uvicorn
import structlog

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from chartop_server.routers import timeseries, tags, metrics
from chartop_server.controllers.tsdb.factory import TSDBControllerContainer
from chartop_server.controllers.tsdb.exceptions import TSDBControllerException


@asynccontextmanager
async def lifespan(app: FastAPI):
    await TSDBControllerContainer.init_controller()
    yield
    controller = TSDBControllerContainer.get_controller()
    await controller.cleanup()


app = FastAPI(tags=["chartop"], lifespan=lifespan)


@app.exception_handler(TSDBControllerException)
async def tsdb_controller_exception_handler(
    request: Request, exc: TSDBControllerException
):
    logger = structlog.getLogger(component="tsdb controller exception handler")
    logger.error(f"Unexpected TSDB controller error occurred: {exc}", exc_info=True)
    return JSONResponse(
        status_code=exc.http_status_code,
        content={"success": False, "message": "Unexpected error occurred."},
    )


@app.exception_handler(Exception)
async def uvicorn_exception_handler(request: Request, exc: Exception):
    logger = structlog.getLogger(component="uvicorn exception handler")
    logger.error(f"Unexpected error occurred: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "message": "Unexpected internal server error occurred.",
        },
    )


app.include_router(tags.router)
app.include_router(metrics.router)
app.include_router(timeseries.router)


app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("ALLOW_ORIGINS").split(",")
    if os.getenv("ALLOW_ORIGINS", None)
    else [],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8443)

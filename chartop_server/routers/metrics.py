from fastapi import APIRouter
from chartop_server.controllers.tsdb.factory import TSDBControllerContainer
from chartop_server.models import MetricsResponse


router = APIRouter(prefix="/api/v1", tags=["metrics"])


@router.get("/metrics")
async def get_metrics() -> MetricsResponse:
    controller = TSDBControllerContainer.get_controller()
    return await controller.get_metrics()

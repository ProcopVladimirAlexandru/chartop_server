from fastapi import APIRouter
from chartop_server.controllers.tsdb.factory import TSDBControllerContainer
from chartop_server.models import TagsResponse


router = APIRouter(prefix="/api/v1", tags=["tags"])


@router.get("/tags")
async def get_tags() -> TagsResponse:
    controller = TSDBControllerContainer.get_controller()
    return await controller.get_tags()

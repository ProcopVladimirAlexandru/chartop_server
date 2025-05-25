from fastapi import APIRouter, Query
from chartop_server.controllers.tsdb.factory import TSDBControllerContainer
from chartop_server.models import TimeseriesResponse
from pva_tsdb_connector.enums import AllOrAnyTags


router = APIRouter(prefix="/api/v1", tags=["timeseries", "paginated", "get"])


@router.get("/timeseries", tags=["timeseries_data"])
async def get_timeseries(
    page_number: int = Query(default=0, title="Page Number", ge=0, le=4, example=0),
    page_size: int = Query(default=5, title="Page Size", ge=1, le=50, example=10),
    order_by: int = Query(title="Metric to Order By"),
    order_asc: bool = Query(default=False, title="Ascending Order"),
    tags: list[int] = Query(default=None, title="Filter by These Tags"),
    all_or_any_tags: AllOrAnyTags = Query(
        default=AllOrAnyTags.ANY, title="Match All or Any Tags"
    ),
) -> TimeseriesResponse:
    controller = TSDBControllerContainer.get_controller()
    return await controller.get_timeseries(
        page_number=page_number,
        page_size=page_size,
        order_by=order_by,
        order_asc=order_asc,
        tags=tags,
        all_or_any_tags=all_or_any_tags,
    )

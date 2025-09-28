import os
import datetime

from fastapi import APIRouter, Query
from chartop_server.controllers.tsdb.factory import TSDBControllerContainer
from chartop_server.models import ChartopResponse, VisualizationVectorsResponse
from pva_tsdb_connector.enums import AllOrAnyTags

from chartop_server.utils.utils import get_now

router = APIRouter(prefix="/api/v1", tags=["timeseries"])


@router.get("/chartop")
async def get_chartop(
    page_number: int = Query(default=0, title="Page Number", ge=0, le=4, example=0),
    page_size: int = Query(default=5, title="Page Size", ge=1, le=50, example=10),
    order_by: int = Query(title="Metric to Order By"),
    order_asc: bool = Query(default=False, title="Ascending Order"),
    tags: list[int] = Query(default=None, title="Filter by These Tags"),
    all_or_any_tags: AllOrAnyTags = Query(
        default=AllOrAnyTags.ANY, title="Match All or Any Tags"
    ),
) -> ChartopResponse:
    controller = TSDBControllerContainer.get_controller()
    return await controller.get_chartop(
        page_number=page_number,
        page_size=page_size,
        order_by=order_by,
        order_asc=order_asc,
        tags=tags,
        all_or_any_tags=all_or_any_tags,
    )


@router.get("/visualization_vectors")
async def get_visualization_vectors(
    origin_vector: list[float] | None = Query(
        default=None,
        title="Vector Origin of Search",
        description="Exclusive with origin_ts_uid.",
    ),
    origin_ts_uid: int | None = Query(
        default=None,
        title="TS Origin of Search",
        description="Exclusive with origin_vector.",
    ),
    radius: float = Query(title="Radius of Search", example=2.5, gt=0.0),
    limit: int = Query(title="Limit", ge=0, le=250, example=50),
    exclude_ts_uids: list[int] | None = Query(default=None, title="Excluded TS UIDs"),
) -> VisualizationVectorsResponse:
    controller = TSDBControllerContainer.get_controller()
    return await controller.get_visualization_vectors(
        origin_vector=origin_vector,
        origin_ts_uid=origin_ts_uid,
        radius=radius,
        limit=limit,
        exclude_ts_uids=exclude_ts_uids,
        start_date=get_now()
        - datetime.timedelta(
            days=int(os.getenv("VISUALIZATION_VECTORS_TS_START_DATE_DAYS_DIFF", "90"))
        ),
    )

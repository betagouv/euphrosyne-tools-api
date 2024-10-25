import os
import logging
import urllib
import httpx

from fastapi import APIRouter, Depends
from starlette.background import BackgroundTask
from starlette.requests import Request
from starlette.responses import StreamingResponse

from auth import verify_authenticated_query_url_jwt

logger = logging.getLogger(__name__)

if not os.getenv("EROS_BASE_URL"):
    logger.warning("EROS_BASE_URL not set, not including eros routes")


router = APIRouter(prefix="/eros", tags=["eros"])


async def _reverse_proxy(request: Request, path: str):
    client = httpx.AsyncClient()
    query_params = {
        **request.query_params,
        "token": os.environ["EROS_API_TOKEN"],
    }
    url = httpx.URL(
        os.environ["EROS_BASE_URL"],
        path=f"/{path}",
        query=urllib.parse.urlencode(
            query_params,
        ).encode("utf-8"),
    )

    rp_req = client.build_request(
        request.method, url, headers=request.headers.raw, content=request.stream()
    )
    rp_resp = await client.send(rp_req, stream=True)
    return StreamingResponse(
        rp_resp.aiter_raw(),
        status_code=rp_resp.status_code,
        headers=rp_resp.headers,
        background=BackgroundTask(rp_resp.aclose),
    )


@router.get("/{path:path}", dependencies=[Depends(verify_authenticated_query_url_jwt)])
@router.post("/{path:path}", dependencies=[Depends(verify_authenticated_query_url_jwt)])
async def eros_proxy(request: Request, path: str):
    return await _reverse_proxy(request, path)

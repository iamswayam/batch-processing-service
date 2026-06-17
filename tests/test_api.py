import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_invalid_content_length_returns_400(client: AsyncClient):
    response = await client.get("/healthz", headers={"Content-Length": "not-a-number"})

    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid Content-Length header"


@pytest.mark.asyncio
async def test_oversized_content_length_returns_413(client: AsyncClient):
    response = await client.get("/healthz", headers={"Content-Length": str(10 * 1024 * 1024 + 1)})

    assert response.status_code == 413
    assert response.json()["detail"] == "Request body too large"

from fastapi import Header, HTTPException


async def get_tenant_id(x_tenant_id: str = Header(...)) -> str:
    if not x_tenant_id.strip():
        raise HTTPException(status_code=400, detail="X-Tenant-ID header is required")
    return x_tenant_id.strip()
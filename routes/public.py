from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import logging
import hashlib
import uuid

from database import get_db
from models import QRCode, QRScan, SocialClick
from utils import parse_device_info, get_location_from_ip, get_location_from_gps
from config import settings

router = APIRouter(tags=["Public"])
logger = logging.getLogger(__name__)


def generate_session_id(ip: str, user_agent: str) -> str:
    """Generate consistent session ID from IP and user agent - BACKEND FALLBACK"""
    data = f"{ip}:{user_agent}"
    return hashlib.sha256(data.encode()).hexdigest()[:32]


async def is_new_user(db: AsyncSession, session_id: str) -> bool:
    qr_result = await db.execute(
        select(QRScan.id).where(QRScan.session_id == session_id).limit(1)
    )
    if qr_result.scalar_one_or_none():
        return False

    social_result = await db.execute(
        select(SocialClick.id).where(SocialClick.session_id == session_id).limit(1)
    )
    return social_result.scalar_one_or_none() is None



@router.get("/r/{code}")
async def redirect_qr(code: str, request: Request, db: AsyncSession = Depends(get_db)):
    try:
        result = await db.execute(
            select(QRCode.id, QRCode.target_url, QRCode.is_active, QRCode.code)
            .where(QRCode.code == code)
        )
        qr_data = result.one_or_none()

        if not qr_data:
            raise HTTPException(status_code=404, detail="QR code not found")

        qr_id, target_url, is_active, qr_code = qr_data

        if not is_active:
            raise HTTPException(status_code=410, detail="QR code deactivated")

        separator = "&" if "?" in target_url else "?"
        redirect_url = f"{target_url}{separator}branch={qr_code}"

        # ✅ FIX: CREATE OR REUSE SESSION ID *BEFORE* RENDERING HTML
        session_id = request.cookies.get("qr_session") or str(uuid.uuid4())

        html_content = f"""<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Redirecting...</title>
</head>
<body>
<script>
const QR_ID = {qr_id};
const TARGET_URL = "{redirect_url}";
const API = "{settings.BASE_URL}";
const SESSION_ID = "{session_id}";  // ✅ INJECTED FROM BACKEND - NO RACE CONDITION

function sendLog(lat, lon, accuracy) {{
    const payload = JSON.stringify({{
        qr_code_id: QR_ID,
        latitude: lat,
        longitude: lon,
        accuracy: accuracy,
        user_agent: navigator.userAgent,
        session_id: SESSION_ID  // ✅ USE INJECTED SESSION
    }});

    if (navigator.sendBeacon) {{
        navigator.sendBeacon(`${{API}}/api/scan-log`, payload);
    }} else {{
        fetch(`${{API}}/api/scan-log`, {{
            method: "POST",
            credentials: "include",
            headers: {{ "Content-Type": "application/json" }},
            body: payload
        }});
    }}
}}

// Try GPS but DO NOT WAIT
if (navigator.geolocation) {{
    navigator.geolocation.getCurrentPosition(
        pos => sendLog(pos.coords.latitude, pos.coords.longitude, pos.coords.accuracy),
        () => sendLog(null, null, null),
        {{ timeout: 2000 }}
    );
}} else {{
    sendLog(null, null, null);
}}

// Instant redirect — no waiting
window.location.replace(TARGET_URL);
</script>
</body>
</html>"""

        response = HTMLResponse(content=html_content)

        # ✅ Set cookie with the SAME session_id we injected
        response.set_cookie(
            key="qr_session",
            value=session_id,
            max_age=60 * 60 * 24 * 365,
            httponly=False,  # ✅ Changed to False so JavaScript can read it
            samesite="None",
            secure=True,
            path="/"
        )

        return response

    except Exception as e:
        logger.error(f"Redirect error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal error")

@router.post("/api/scan-log")
async def log_scan(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        data = await request.json()

        qr_code_id = data.get("qr_code_id")
        latitude = data.get("latitude")
        longitude = data.get("longitude")
        accuracy = data.get("accuracy")
        user_agent = data.get("user_agent", "")
        frontend_session = data.get("session_id", "")

        ip_address = request.client.host if request.client else None
        cookie_session = request.cookies.get("qr_session")

        # ✅ FIXED: Strict priority - frontend session (injected) takes precedence
        if frontend_session:
            session_id = frontend_session
        elif cookie_session:
            session_id = cookie_session
        else:
            # This should NEVER happen now, but keep as safety net
            session_id = str(uuid.uuid4())
            logger.warning(f"No session found for QR scan {qr_code_id}, created emergency fallback")

        device_info = parse_device_info(user_agent)
        is_new = await is_new_user(db, session_id)

        if latitude and longitude:
            location_data = await get_location_from_gps(latitude, longitude)
        else:
            location_data = await get_location_from_ip(ip_address)

        scan = QRScan(
            qr_code_id=qr_code_id,
            device_type=device_info["device_type"],
            device_name=device_info["device_name"],
            browser=device_info["browser"],
            os=device_info["os"],
            ip_address=ip_address,
            country=location_data.get("country") if location_data else None,
            city=location_data.get("city") if location_data else None,
            region=location_data.get("region") if location_data else None,
            session_id=session_id,
            is_new_user=is_new,
            user_agent=user_agent
        )

        db.add(scan)
        await db.commit()

        return {"status": "success", "is_new_user": is_new}

    except Exception as e:
        logger.error(f"Scan log error: {e}", exc_info=True)
        return {"status": "error"}
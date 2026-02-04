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
    """Generate consistent session ID from IP and user agent"""
    data = f"{ip}:{user_agent}"
    return hashlib.sha256(data.encode()).hexdigest()[:32]


async def is_new_user(db: AsyncSession, session_id: str) -> bool:
    """
    Check if this is a new user based on session_id.
    Checks BOTH QR scans AND social clicks across all branches.
    """
    # Check QR scans
    qr_result = await db.execute(
        select(QRScan.id).where(QRScan.session_id == session_id).limit(1)
    )
    qr_exists = qr_result.scalar_one_or_none()
    
    if qr_exists:
        return False  # Found in QR scans - returning user
    
    # Check social clicks
    social_result = await db.execute(
        select(SocialClick.id).where(SocialClick.session_id == session_id).limit(1)
    )
    social_exists = social_result.scalar_one_or_none()
    
    return social_exists is None  # New only if not found in both tables


@router.get("/r/{code}")
async def redirect_qr(
    code: str,
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """
    Show GPS permission page, then redirect to target URL with branch parameter.
    Track new vs returning users via session.
    """
    try:
        result = await db.execute(
            select(QRCode.id, QRCode.target_url, QRCode.is_active, QRCode.code, QRCode.branch_id)
            .where(QRCode.code == code)
        )
        qr_data = result.one_or_none()
        
        if not qr_data:
            raise HTTPException(status_code=404, detail=f"QR code '{code}' not found")
        
        qr_id, target_url, is_active, qr_code, branch_id = qr_data
        
        if not is_active:
            raise HTTPException(status_code=410, detail="This QR code has been deactivated")
        
        # Append branch parameter to target URL
        separator = "&" if "?" in target_url else "?"
        redirect_url = f"{target_url}{separator}branch={qr_code}"
        
        # Return GPS permission page with session tracking
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Redirecting...</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0;
            padding: 20px;
        }}
        .container {{
            background: white;
            border-radius: 16px;
            padding: 40px;
            max-width: 400px;
            text-align: center;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
        }}
        .icon {{ font-size: 48px; margin-bottom: 16px; }}
        h1 {{ color: #1a202c; margin-bottom: 8px; font-size: 20px; }}
        p {{ color: #718096; font-size: 14px; margin-bottom: 20px; }}
        .spinner {{
            border: 3px solid #e2e8f0;
            border-top: 3px solid #667eea;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin: 20px auto;
        }}
        @keyframes spin {{
            0% {{ transform: rotate(0deg); }}
            100% {{ transform: rotate(360deg); }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="icon">📍</div>
        <h1>Redirecting...</h1>
        <p id="status">Detecting your location...</p>
        <div class="spinner"></div>
    </div>
    
    <script>
        const QR_ID = {qr_id};
        const TARGET_URL = "{redirect_url}";
        const API = "{settings.BASE_URL}";
        
        // Generate session ID from browser fingerprint
        function generateSessionId() {{
            const canvas = document.createElement('canvas');
            const ctx = canvas.getContext('2d');
            ctx.textBaseline = 'top';
            ctx.font = '14px Arial';
            ctx.fillText('fingerprint', 2, 2);
            const fingerprint = canvas.toDataURL();
            
            const data = navigator.userAgent + fingerprint + screen.width + screen.height;
            let hash = 0;
            for (let i = 0; i < data.length; i++) {{
                const char = data.charCodeAt(i);
                hash = ((hash << 5) - hash) + char;
                hash = hash & hash;
            }}
            return Math.abs(hash).toString(16);
        }}
        
        async function logScan(lat, lon, accuracy) {{
            const userAgent = navigator.userAgent;
            const sessionId = generateSessionId();
            
            try {{
                await fetch(`${{API}}/api/scan-log`, {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{
                        qr_code_id: QR_ID,
                        latitude: lat,
                        longitude: lon,
                        accuracy: accuracy,
                        user_agent: userAgent,
                        session_id: sessionId
                    }})
                }});
            }} catch (e) {{
                console.log('Log failed:', e);
            }}
            
            window.location.href = TARGET_URL;
        }}
        
        if (navigator.geolocation) {{
            navigator.geolocation.getCurrentPosition(
                (position) => {{
                    logScan(
                        position.coords.latitude,
                        position.coords.longitude,
                        position.coords.accuracy
                    );
                }},
                (error) => {{
                    document.getElementById('status').textContent = 'Redirecting...';
                    logScan(null, null, null);
                }},
                {{ timeout: 5000 }}
            );
        }} else {{
            logScan(null, null, null);
        }}
    </script>
</body>
</html>
        """
        
        return HTMLResponse(content=html_content)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in redirect_qr: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/scan-log")
async def log_scan(
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """Log QR scan with GPS coordinates and session tracking"""
    try:
        data = await request.json()
        qr_code_id = data.get("qr_code_id")
        latitude = data.get("latitude")
        longitude = data.get("longitude")
        accuracy = data.get("accuracy")
        user_agent = data.get("user_agent", "")
        session_id = data.get("session_id", str(uuid.uuid4()))
        
        ip_address = request.client.host if request.client else None
        device_info = parse_device_info(user_agent)
        
        # Check if new user
        is_new = await is_new_user(db, session_id)
        
        # Get location - GPS if available, else IP
        location_data = None
        if latitude and longitude:
            logger.info(f"Using GPS: lat={latitude}, lon={longitude}, accuracy={accuracy}m")
            location_data = await get_location_from_gps(latitude, longitude)
        else:
            logger.info(f"GPS not available, using IP: {ip_address}")
            location_data = await get_location_from_ip(ip_address)
        
        # Create scan record
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
        
        user_type = "New" if is_new else "Returning"
        logger.info(f"Scan logged: QR {qr_code_id}, {user_type} user, Location: {scan.city}, {scan.country}")
        
        return {"status": "success", "is_new_user": is_new}
        
    except Exception as e:
        logger.error(f"Error logging scan: {str(e)}", exc_info=True)
        return {"status": "error"}
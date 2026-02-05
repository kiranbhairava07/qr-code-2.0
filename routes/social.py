from fastapi import APIRouter, Request, Depends, Query
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from pathlib import Path
from typing import Optional
import logging
import hashlib

from database import get_db
from models import SocialClick, QRCode, QRScan
from utils import parse_device_info, get_location_from_ip

router = APIRouter(tags=["Social Links"])
logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path("templates/social")


def generate_session_id(ip: str, user_agent: str) -> str:
    """Generate consistent session ID from IP and user agent - BACKEND FALLBACK"""
    data = f"{ip}:{user_agent}"
    return hashlib.sha256(data.encode()).hexdigest()[:32]


@router.get("/social-links", response_class=HTMLResponse)
async def social_links_page(
    request: Request,
    branch: Optional[str] = Query(None, description="Branch identifier from QR code")
):
    """
    Serve the social media links page.
    Branch can be passed as query parameter: /social-links?branch=main-branch-2024
    """
    try:
        html_path = TEMPLATES_DIR / "index.html"
        
        if not html_path.exists():
            return HTMLResponse(
                content="<h1>Social Links page not found</h1>",
                status_code=404
            )
        
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Inject branch parameter into the page
        if branch:
            html_content = html_content.replace(
                'const BRANCH_CODE = null;',
                f'const BRANCH_CODE = "{branch}";'
            )
        
        return HTMLResponse(content=html_content)
    
    except Exception as e:
        logger.error(f"Error loading social links page: {str(e)}", exc_info=True)
        return HTMLResponse(
            content=f"<h1>Error loading page: {str(e)}</h1>",
            status_code=500
        )



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


@router.post("/api/social-click")
async def log_social_click(
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """Track clicks on social media platform buttons with new vs returning tracking"""
    try:
        data = await request.json()
        platform = data.get("platform", "unknown")
        branch_code = data.get("branch_code")
        frontend_session = data.get("session_id", "")
        user_agent = request.headers.get("user-agent", "")
        
        # Get IP address
        ip_address = request.client.host if request.client else None
        
        # SIMPLE ELEGANT FIX: Use backend IP+UA hash if frontend session fails
        if frontend_session and len(frontend_session) > 10:
            session_id = frontend_session
        else:
            session_id = generate_session_id(ip_address or "unknown", user_agent)
            logger.info(f"Using backend session ID for social click (localStorage failed): {session_id[:16]}...")
        
        # Get branch_id from QR code
        branch_id = None
        if branch_code:
            result = await db.execute(
                select(QRCode.branch_id).where(QRCode.code == branch_code)
            )
            branch_id = result.scalar_one_or_none()
        
        # Parse device info
        device_info = parse_device_info(user_agent)
        
        # Get location from IP
        location_data = await get_location_from_ip(ip_address)
        
        # Check if new user
        is_new = await is_new_user(db, session_id)
        
        # Create click record
        click = SocialClick(
            platform=platform,
            branch_id=branch_id,
            device_type=device_info["device_type"],
            browser=device_info["browser"],
            os=device_info["os"],
            ip_address=ip_address,
            country=location_data.get("country") if location_data else None,
            city=location_data.get("city") if location_data else None,
            session_id=session_id,
            is_new_user=is_new,
            user_agent=user_agent
        )
        
        db.add(click)
        await db.commit()
        
        user_type = "New" if is_new else "Returning"
        logger.info(f"Social click logged: {platform} from branch_id={branch_id}, {user_type} user, Session: {session_id[:16]}...")
        
        return {"status": "success", "is_new_user": is_new}
        
    except Exception as e:
        logger.error(f"Error logging social click: {str(e)}", exc_info=True)
        return {"status": "error", "message": str(e)}



@router.get("/api/social-analytics")
async def get_social_analytics(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    branch_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Get analytics for social media platform clicks.
    Can filter by branch_id, start_date, and end_date.
    """
    try:
        from datetime import datetime, timedelta
        
        # Build filters
        filters = []
        
        if branch_id:
            filters.append(SocialClick.branch_id == branch_id)
        
        if start_date:
            try:
                start_dt = datetime.fromisoformat(start_date)
                filters.append(SocialClick.clicked_at >= start_dt)
            except ValueError:
                logger.warning(f"Invalid start_date format: {start_date}")
        
        if end_date:
            try:
                end_dt = datetime.fromisoformat(end_date)
                end_dt = end_dt + timedelta(days=1) - timedelta(seconds=1)
                filters.append(SocialClick.clicked_at <= end_dt)
            except ValueError:
                logger.warning(f"Invalid end_date format: {end_date}")
        
        # Get total clicks per platform with filters
        query = select(
            SocialClick.platform,
            func.count(SocialClick.id).label('count')
        ).group_by(SocialClick.platform).order_by(func.count(SocialClick.id).desc())
        
        if filters:
            from sqlalchemy import and_
            query = query.where(and_(*filters))
        
        result = await db.execute(query)
        
        platform_stats = [
            {"platform": row.platform, "count": row.count}
            for row in result.all()
        ]
        
        # Get total clicks with filters
        total_query = select(func.count(SocialClick.id))
        if filters:
            total_query = total_query.where(and_(*filters))
        
        total_result = await db.execute(total_query)
        total_clicks = total_result.scalar()
        
        return {
            "total_clicks": total_clicks or 0,
            "platforms": platform_stats,
            "branch_id": branch_id
        }
        
    except Exception as e:
        logger.error(f"Error getting social analytics: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": "Failed to get analytics"}
        )


@router.get("/social-links/styles.css")
async def social_links_css():
    """Serve the CSS file for social links page"""
    try:
        css_path = TEMPLATES_DIR / "styles.css"
        
        if not css_path.exists():
            return HTMLResponse(content="", status_code=404)
        
        with open(css_path, 'r', encoding='utf-8') as f:
            css_content = f.read()
        
        return HTMLResponse(
            content=css_content,
            media_type="text/css"
        )
    except Exception as e:
        logger.error(f"Error loading CSS: {str(e)}", exc_info=True)
        return HTMLResponse(content="", status_code=500)


@router.get("/social-links/{image_name}")
async def social_links_images(image_name: str):
    """Serve images for social links page"""
    from fastapi.responses import FileResponse
    
    try:
        allowed_images = [
            'gk.png', 'facebook.png', 'instagram.png', 
            'youtube.png', 'threads.png', 'twitter.png', 'whatsapp.png'
        ]
        
        if image_name not in allowed_images:
            return HTMLResponse(content="Not found", status_code=404)
        
        image_path = TEMPLATES_DIR / image_name
        
        if not image_path.exists():
            return HTMLResponse(content="Image not found", status_code=404)
        
        if image_name.endswith('.png'):
            media_type = "image/png"
        elif image_name.endswith('.jpg') or image_name.endswith('.jpeg'):
            media_type = "image/jpeg"
        else:
            media_type = "application/octet-stream"
        
        return FileResponse(
            path=image_path,
            media_type=media_type
        )
    except Exception as e:
        logger.error(f"Error loading image: {str(e)}", exc_info=True)
        return HTMLResponse(content="Error", status_code=500)
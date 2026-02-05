from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_
from typing import List, Optional
from datetime import datetime, timedelta

from database import get_db
from auth import get_current_user
from models import User, Region, Cluster, Branch, QRCode, QRScan, SocialClick
from schemas import (
    RegionAnalytics, ClusterAnalytics, BranchAnalytics,
    NewVsReturning, SocialAnalytics
)

router = APIRouter(prefix="/analytics", tags=["Analytics"])


def require_super_admin(current_user: User):
    """Helper to check if user is super admin"""
    if not current_user.is_super_admin:
        raise HTTPException(
            status_code=403,
            detail="Only super admin can view analytics"
        )


async def calculate_new_vs_returning_bulk(
    db: AsyncSession,
    region_ids: List[int],
    qr_filters=None,
    social_filters=None
) -> dict[int, NewVsReturning]:
    """
    Bulk calculation of new vs returning users grouped by region.
    Returns: { region_id: NewVsReturning }
    """

    qr_filters = qr_filters or []
    social_filters = social_filters or []

    # ---------------- QR DATA ----------------
    qr_stmt = (
        select(
            Cluster.region_id,
            func.sum(func.case((QRScan.is_new_user == True, 1), else_=0)).label("new_users"),
            func.sum(func.case((QRScan.is_new_user == False, 1), else_=0)).label("returning_users"),
        )
        .join(QRCode, QRCode.id == QRScan.qr_code_id)
        .join(Branch, Branch.id == QRCode.branch_id)
        .join(Cluster, Cluster.id == Branch.cluster_id)
        .where(Cluster.region_id.in_(region_ids))
        .group_by(Cluster.region_id)
    )

    if qr_filters:
        qr_stmt = qr_stmt.where(and_(*qr_filters))

    qr_rows = (await db.execute(qr_stmt)).all()

    # ---------------- SOCIAL DATA ----------------
    social_stmt = (
        select(
            Cluster.region_id,
            func.sum(func.case((SocialClick.is_new_user == True, 1), else_=0)).label("new_users"),
            func.sum(func.case((SocialClick.is_new_user == False, 1), else_=0)).label("returning_users"),
        )
        .join(Branch, Branch.id == SocialClick.branch_id)
        .join(Cluster, Cluster.id == Branch.cluster_id)
        .where(Cluster.region_id.in_(region_ids))
        .group_by(Cluster.region_id)
    )

    if social_filters:
        social_stmt = social_stmt.where(and_(*social_filters))

    social_rows = (await db.execute(social_stmt)).all()

    # ---------------- MERGE RESULTS ----------------
    from collections import defaultdict
    result_map = defaultdict(lambda: {"new": 0, "ret": 0})

    for rid, new_u, ret_u in qr_rows:
        result_map[rid]["new"] += new_u or 0
        result_map[rid]["ret"] += ret_u or 0

    for rid, new_u, ret_u in social_rows:
        result_map[rid]["new"] += new_u or 0
        result_map[rid]["ret"] += ret_u or 0

    final = {}
    for rid in region_ids:
        new_users = result_map[rid]["new"]
        returning_users = result_map[rid]["ret"]
        total = new_users + returning_users

        final[rid] = NewVsReturning(
            new_users=new_users,
            returning_users=returning_users,
            new_percentage=round((new_users / total * 100), 2) if total else 0.0,
            returning_percentage=round((returning_users / total * 100), 2) if total else 0.0
        )

    return final

# ============================================
# REGION ANALYTICS
# ============================================
@router.get("/regions", response_model=List[RegionAnalytics])
async def get_region_analytics(
    region_id: Optional[int] = Query(None, description="Specific region ID"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_details: bool = Query(False, description="Include cluster and branch breakdown"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    require_super_admin(current_user)

    # ---------------- DATE FILTERS ----------------
    qr_filters = []
    social_filters = []

    try:
        if start_date:
            start_dt = datetime.fromisoformat(start_date)
            qr_filters.append(QRScan.scanned_at >= start_dt)
            social_filters.append(SocialClick.clicked_at >= start_dt)

        if end_date:
            end_dt = datetime.fromisoformat(end_date) + timedelta(days=1) - timedelta(seconds=1)
            qr_filters.append(QRScan.scanned_at <= end_dt)
            social_filters.append(SocialClick.clicked_at <= end_dt)
    except ValueError:
        pass

    # ---------------- FETCH REGIONS ----------------
    region_stmt = select(Region.id, Region.name).where(Region.is_active == True)
    if region_id:
        region_stmt = region_stmt.where(Region.id == region_id)

    region_rows = (await db.execute(region_stmt.order_by(Region.name))).all()
    region_map = {r.id: r.name for r in region_rows}

    if not region_map:
        return []

    # ---------------- BRANCHES GROUPED BY REGION ----------------
    branch_stmt = (
        select(Cluster.region_id, Branch.id)
        .join(Branch, Branch.cluster_id == Cluster.id)
        .where(Branch.is_active == True, Cluster.region_id.in_(region_map.keys()))
    )

    branch_rows = (await db.execute(branch_stmt)).all()

    branches_by_region = {}
    for rid, bid in branch_rows:
        branches_by_region.setdefault(rid, []).append(bid)

    # ---------------- QR SCANS GROUPED BY REGION ----------------
    qr_stmt = (
        select(Cluster.region_id, func.count(QRScan.id))
        .join(Branch, Branch.cluster_id == Cluster.id)
        .join(QRCode, QRCode.branch_id == Branch.id)
        .join(QRScan, QRScan.qr_code_id == QRCode.id)
        .where(Cluster.region_id.in_(region_map.keys()))
        .group_by(Cluster.region_id)
    )

    if qr_filters:
        qr_stmt = qr_stmt.where(and_(*qr_filters))

    qr_counts = dict((await db.execute(qr_stmt)).all())

    # ---------------- SOCIAL CLICKS GROUPED BY REGION ----------------
    social_stmt = (
        select(Cluster.region_id, func.count(SocialClick.id))
        .join(Branch, Branch.cluster_id == Cluster.id)
        .join(SocialClick, SocialClick.branch_id == Branch.id)
        .where(Cluster.region_id.in_(region_map.keys()))
        .group_by(Cluster.region_id)
    )

    if social_filters:
        social_stmt = social_stmt.where(and_(*social_filters))

    social_counts = dict((await db.execute(social_stmt)).all())

    # ---------------- BULK NEW VS RETURNING ----------------
    new_returning_map = await calculate_new_vs_returning_bulk(
        db=db,
        region_ids=list(region_map.keys()),
        qr_filters=qr_filters,
        social_filters=social_filters
    )

    # ---------------- BUILD RESPONSE ----------------
    analytics = []

    for rid, rname in region_map.items():
        total_qr = qr_counts.get(rid, 0)
        total_social = social_counts.get(rid, 0)

        region_analytics = RegionAnalytics(
            region_id=rid,
            region_name=rname,
            total_qr_scans=total_qr,
            total_social_clicks=total_social,
            combined_total=total_qr + total_social,
            new_vs_returning=new_returning_map.get(
                rid,
                NewVsReturning(
                    new_users=0,
                    returning_users=0,
                    new_percentage=0.0,
                    returning_percentage=0.0
                )
            ),
            clusters=[]
        )

        # Only load cluster details if requested (kept separate to avoid slowing main endpoint)
        if include_details:
            region_analytics.clusters = await get_cluster_analytics_internal(
                db=db,
                region_id=rid,
                start_date=start_date,
                end_date=end_date
            )

        analytics.append(region_analytics)

    return analytics


# ============================================
# CLUSTER ANALYTICS
# ============================================
async def get_cluster_analytics_internal(
    db: AsyncSession,
    cluster: Cluster,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_branches: bool = False
) -> ClusterAnalytics:
    """Internal function to get cluster analytics"""
    
    # Build date filters for both QR and Social
    qr_date_filters = []
    social_date_filters = []
    
    if start_date:
        try:
            start_dt = datetime.fromisoformat(start_date)
            qr_date_filters.append(QRScan.scanned_at >= start_dt)
            social_date_filters.append(SocialClick.clicked_at >= start_dt)
        except ValueError:
            pass
    
    if end_date:
        try:
            end_dt = datetime.fromisoformat(end_date) + timedelta(days=1) - timedelta(seconds=1)
            qr_date_filters.append(QRScan.scanned_at <= end_dt)
            social_date_filters.append(SocialClick.clicked_at <= end_dt)
        except ValueError:
            pass
    
    # Get all branches in this cluster
    branch_ids_query = select(Branch.id).where(
        Branch.cluster_id == cluster.id,
        Branch.is_active == True
    )
    branch_ids_result = await db.execute(branch_ids_query)
    branch_ids = [row[0] for row in branch_ids_result.all()]
    
    if not branch_ids:
        # Return empty analytics if no branches
        return ClusterAnalytics(
            cluster_id=cluster.id,
            cluster_name=cluster.name,
            region_id=cluster.region_id,
            total_qr_scans=0,
            total_social_clicks=0,
            combined_total=0,
            new_vs_returning=NewVsReturning(
                new_users=0,
                returning_users=0,
                new_percentage=0.0,
                returning_percentage=0.0
            ),
            branches=[]
        )
    
    # QR Scans
    qr_query = select(func.count(QRScan.id)).join(QRCode).where(
        QRCode.branch_id.in_(branch_ids)
    )
    if qr_date_filters:
        qr_query = qr_query.where(and_(*qr_date_filters))
    
    total_qr_scans = (await db.execute(qr_query)).scalar() or 0
    
    # Social Clicks
    social_query = select(func.count(SocialClick.id)).where(
        SocialClick.branch_id.in_(branch_ids)
    )
    if social_date_filters:
        social_query = social_query.where(and_(*social_date_filters))
    
    total_social_clicks = (await db.execute(social_query)).scalar() or 0
    
    # New vs Returning - BUILD QUERY OBJECTS, NOT FILTER LISTS
    qr_base_query = select(QRScan).join(QRCode).where(QRCode.branch_id.in_(branch_ids))
    social_base_query = select(SocialClick).where(SocialClick.branch_id.in_(branch_ids))
    
    # Add date filters if they exist
    if qr_date_filters:
        qr_base_query = qr_base_query.where(and_(*qr_date_filters))
    
    if social_date_filters:
        social_base_query = social_base_query.where(and_(*social_date_filters))
    
    new_vs_returning = await calculate_new_vs_returning(
        db, qr_base_query, social_base_query
    )
    
    cluster_analytics = ClusterAnalytics(
        cluster_id=cluster.id,
        cluster_name=cluster.name,
        region_id=cluster.region_id,
        total_qr_scans=total_qr_scans,
        total_social_clicks=total_social_clicks,
        combined_total=total_qr_scans + total_social_clicks,
        new_vs_returning=new_vs_returning,
        branches=[]
    )
    
    # Include branch details if requested
    if include_branches:
        branches_result = await db.execute(
            select(Branch).where(
                Branch.cluster_id == cluster.id,
                Branch.is_active == True
            ).order_by(Branch.name)
        )
        branches = branches_result.scalars().all()
        
        for branch in branches:
            branch_analytics = await get_branch_analytics_internal(
                db, branch, start_date, end_date
            )
            cluster_analytics.branches.append(branch_analytics)
    
    return cluster_analytics

@router.get("/clusters/{cluster_id}", response_model=ClusterAnalytics)
async def get_cluster_analytics(
    cluster_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_branches: bool = Query(True, description="Include branch breakdown"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get analytics for a specific cluster"""
    require_super_admin(current_user)
    
    result = await db.execute(
        select(Cluster).where(Cluster.id == cluster_id)
    )
    cluster = result.scalar_one_or_none()
    
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    
    return await get_cluster_analytics_internal(
        db, cluster, start_date, end_date, include_branches
    )


# ============================================
# BRANCH ANALYTICS
# ============================================
async def get_branch_analytics_internal(
    db: AsyncSession,
    branch: Branch,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> BranchAnalytics:
    """Internal function to get branch analytics"""
    
    # Build date filters for both QR and Social
    qr_date_filters = []
    social_date_filters = []
    
    if start_date:
        try:
            start_dt = datetime.fromisoformat(start_date)
            qr_date_filters.append(QRScan.scanned_at >= start_dt)
            social_date_filters.append(SocialClick.clicked_at >= start_dt)
        except ValueError:
            pass
    
    if end_date:
        try:
            end_dt = datetime.fromisoformat(end_date) + timedelta(days=1) - timedelta(seconds=1)
            qr_date_filters.append(QRScan.scanned_at <= end_dt)
            social_date_filters.append(SocialClick.clicked_at <= end_dt)
        except ValueError:
            pass
    
    # QR Scans
    qr_query = select(func.count(QRScan.id)).join(QRCode).where(
        QRCode.branch_id == branch.id
    )
    if qr_date_filters:
        qr_query = qr_query.where(and_(*qr_date_filters))
    
    total_qr_scans = (await db.execute(qr_query)).scalar() or 0
    
    # Social Clicks
    social_query = select(func.count(SocialClick.id)).where(
        SocialClick.branch_id == branch.id
    )
    if social_date_filters:
        social_query = social_query.where(and_(*social_date_filters))
    
    total_social_clicks = (await db.execute(social_query)).scalar() or 0
    
    # New vs Returning - BUILD QUERY OBJECTS, NOT FILTER LISTS
    qr_base_query = select(QRScan).join(QRCode).where(QRCode.branch_id == branch.id)
    social_base_query = select(SocialClick).where(SocialClick.branch_id == branch.id)
    
    # Add date filters if they exist
    if qr_date_filters:
        qr_base_query = qr_base_query.where(and_(*qr_date_filters))
    
    if social_date_filters:
        social_base_query = social_base_query.where(and_(*social_date_filters))
    
    new_vs_returning = await calculate_new_vs_returning(
        db, qr_base_query, social_base_query
    )
    
    return BranchAnalytics(
        branch_id=branch.id,
        branch_name=branch.name,
        cluster_id=branch.cluster_id,
        total_qr_scans=total_qr_scans,
        total_social_clicks=total_social_clicks,
        combined_total=total_qr_scans + total_social_clicks,
        new_vs_returning=new_vs_returning
    )

@router.get("/branches/{branch_id}", response_model=BranchAnalytics)
async def get_branch_analytics(
    branch_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get analytics for a specific branch"""
    require_super_admin(current_user)
    
    result = await db.execute(
        select(Branch).where(Branch.id == branch_id)
    )
    branch = result.scalar_one_or_none()
    
    if not branch:
        raise HTTPException(status_code=404, detail="Branch not found")
    
    return await get_branch_analytics_internal(db, branch, start_date, end_date)


@router.get("/branches/{branch_id}/social-breakdown")
async def get_branch_social_breakdown(
    branch_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get social media platform breakdown for a specific branch"""
    require_super_admin(current_user)
    
    result = await db.execute(select(Branch).where(Branch.id == branch_id))
    branch = result.scalar_one_or_none()
    if not branch:
        raise HTTPException(status_code=404, detail="Branch not found")
    
    query = select(
        SocialClick.platform,
        func.count(SocialClick.id).label('count')
    ).where(SocialClick.branch_id == branch_id)
    
    if start_date:
        try:
            query = query.where(SocialClick.clicked_at >= datetime.fromisoformat(start_date))
        except ValueError:
            pass
    
    if end_date:
        try:
            end_dt = datetime.fromisoformat(end_date) + timedelta(days=1) - timedelta(seconds=1)
            query = query.where(SocialClick.clicked_at <= end_dt)
        except ValueError:
            pass
    
    query = query.group_by(SocialClick.platform).order_by(func.count(SocialClick.id).desc())
    
    result = await db.execute(query)
    breakdown = [{"platform": row.platform, "count": row.count} for row in result.all()]
    
    return {"branch_id": branch_id, "branch_name": branch.name, "platform_breakdown": breakdown}


# ============================================
# SOCIAL MEDIA ANALYTICS
# ============================================
@router.get("/social", response_model=SocialAnalytics)
async def get_social_analytics(
    region_id: Optional[int] = None,
    cluster_id: Optional[int] = None,
    branch_id: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get social media analytics with hierarchical filtering
    """
    require_super_admin(current_user)
    
    # Build base query
    social_base_query = select(SocialClick)
    
    # Hierarchical filtering
    if branch_id:
        social_base_query = social_base_query.where(SocialClick.branch_id == branch_id)
    elif cluster_id:
        # Get all branches in cluster
        branch_ids_query = select(Branch.id).where(Branch.cluster_id == cluster_id)
        branch_ids_result = await db.execute(branch_ids_query)
        branch_ids = [row[0] for row in branch_ids_result.all()]
        social_base_query = social_base_query.where(SocialClick.branch_id.in_(branch_ids))
    elif region_id:
        # Get all branches in region
        branch_ids_query = select(Branch.id).join(Cluster).where(Cluster.region_id == region_id)
        branch_ids_result = await db.execute(branch_ids_query)
        branch_ids = [row[0] for row in branch_ids_result.all()]
        social_base_query = social_base_query.where(SocialClick.branch_id.in_(branch_ids))
    
    # Date filters
    if start_date:
        try:
            social_base_query = social_base_query.where(SocialClick.clicked_at >= datetime.fromisoformat(start_date))
        except ValueError:
            pass
    
    if end_date:
        try:
            end_dt = datetime.fromisoformat(end_date) + timedelta(days=1) - timedelta(seconds=1)
            social_base_query = social_base_query.where(SocialClick.clicked_at <= end_dt)
        except ValueError:
            pass
    
    # Total clicks - use the same base query
    total_query = select(func.count()).select_from(social_base_query.subquery())
    total_clicks = (await db.execute(total_query)).scalar() or 0
    
    # Platform breakdown
    platform_query = select(
        SocialClick.platform,
        func.count(SocialClick.id).label('count')
    )
    
    # Apply the same filters as social_base_query
    if branch_id:
        platform_query = platform_query.where(SocialClick.branch_id == branch_id)
    elif cluster_id:
        branch_ids_query = select(Branch.id).where(Branch.cluster_id == cluster_id)
        branch_ids_result = await db.execute(branch_ids_query)
        branch_ids = [row[0] for row in branch_ids_result.all()]
        platform_query = platform_query.where(SocialClick.branch_id.in_(branch_ids))
    elif region_id:
        branch_ids_query = select(Branch.id).join(Cluster).where(Cluster.region_id == region_id)
        branch_ids_result = await db.execute(branch_ids_query)
        branch_ids = [row[0] for row in branch_ids_result.all()]
        platform_query = platform_query.where(SocialClick.branch_id.in_(branch_ids))
    
    if start_date:
        try:
            platform_query = platform_query.where(SocialClick.clicked_at >= datetime.fromisoformat(start_date))
        except ValueError:
            pass
    
    if end_date:
        try:
            end_dt = datetime.fromisoformat(end_date) + timedelta(days=1) - timedelta(seconds=1)
            platform_query = platform_query.where(SocialClick.clicked_at <= end_dt)
        except ValueError:
            pass
    
    platform_query = platform_query.group_by(SocialClick.platform).order_by(func.count(SocialClick.id).desc())
    
    platform_result = await db.execute(platform_query)
    platform_breakdown = [
        {"platform": row.platform, "count": row.count}
        for row in platform_result.all()
    ]
    
    # New vs Returning - pass query object, not filters list
    new_vs_returning = await calculate_new_vs_returning(
        db, qr_base_query=None, social_base_query=social_base_query
    )
    
    return SocialAnalytics(
        total_clicks=total_clicks,
        new_vs_returning=new_vs_returning,
        platform_breakdown=platform_breakdown,
        region_id=region_id,
        cluster_id=cluster_id,
        branch_id=branch_id
    )
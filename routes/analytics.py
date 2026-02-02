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


async def calculate_new_vs_returning(
    db: AsyncSession,
    qr_filters=None,
    social_filters=None
) -> NewVsReturning:
    """Calculate new vs returning users for QR scans and social clicks"""
    
    # QR Scans
    qr_new_query = select(func.count(QRScan.id)).where(QRScan.is_new_user == True)
    qr_returning_query = select(func.count(QRScan.id)).where(QRScan.is_new_user == False)
    
    if qr_filters:
        qr_new_query = qr_new_query.where(and_(*qr_filters))
        qr_returning_query = qr_returning_query.where(and_(*qr_filters))
    
    qr_new = (await db.execute(qr_new_query)).scalar() or 0
    qr_returning = (await db.execute(qr_returning_query)).scalar() or 0
    
    # Social Clicks
    social_new_query = select(func.count(SocialClick.id)).where(SocialClick.is_new_user == True)
    social_returning_query = select(func.count(SocialClick.id)).where(SocialClick.is_new_user == False)
    
    if social_filters:
        social_new_query = social_new_query.where(and_(*social_filters))
        social_returning_query = social_returning_query.where(and_(*social_filters))
    
    social_new = (await db.execute(social_new_query)).scalar() or 0
    social_returning = (await db.execute(social_returning_query)).scalar() or 0
    
    # Combine
    total_new = qr_new + social_new
    total_returning = qr_returning + social_returning
    total = total_new + total_returning
    
    return NewVsReturning(
        new_users=total_new,
        returning_users=total_returning,
        new_percentage=round((total_new / total * 100), 2) if total > 0 else 0.0,
        returning_percentage=round((total_returning / total * 100), 2) if total > 0 else 0.0
    )


# ============================================
# REGION ANALYTICS
# ============================================
@router.get("/regions", response_model=List[RegionAnalytics])
async def get_region_analytics(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_details: bool = Query(False, description="Include cluster and branch breakdown"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get analytics for all regions
    Optional: start_date, end_date (ISO format)
    """
    require_super_admin(current_user)
    
    # Build date filters
    date_filters = []
    if start_date:
        try:
            start_dt = datetime.fromisoformat(start_date)
            date_filters.append(QRScan.scanned_at >= start_dt)
        except ValueError:
            pass
    
    if end_date:
        try:
            end_dt = datetime.fromisoformat(end_date) + timedelta(days=1) - timedelta(seconds=1)
            date_filters.append(QRScan.scanned_at <= end_dt)
        except ValueError:
            pass
    
    # Get all regions
    regions_result = await db.execute(
        select(Region).where(Region.is_active == True).order_by(Region.name)
    )
    regions = regions_result.scalars().all()
    
    analytics = []
    
    for region in regions:
        # Get all branches in this region
        branch_ids_query = select(Branch.id).join(Cluster).where(
            Cluster.region_id == region.id,
            Branch.is_active == True
        )
        branch_ids_result = await db.execute(branch_ids_query)
        branch_ids = [row[0] for row in branch_ids_result.all()]
        
        if not branch_ids:
            continue
        
        # QR Scans count
        qr_query = select(func.count(QRScan.id)).join(QRCode).where(
            QRCode.branch_id.in_(branch_ids)
        )
        if date_filters:
            qr_query = qr_query.where(and_(*date_filters))
        
        total_qr_scans = (await db.execute(qr_query)).scalar() or 0
        
        # Social Clicks count
        social_query = select(func.count(SocialClick.id)).where(
            SocialClick.branch_id.in_(branch_ids)
        )
        if date_filters:
            social_filters = [
                SocialClick.clicked_at >= date_filters[0].right if len(date_filters) > 0 else None,
                SocialClick.clicked_at <= date_filters[1].right if len(date_filters) > 1 else None
            ]
            social_filters = [f for f in social_filters if f is not None]
            if social_filters:
                social_query = social_query.where(and_(*social_filters))
        
        total_social_clicks = (await db.execute(social_query)).scalar() or 0
        
        # New vs Returning
        qr_filters = [QRCode.branch_id.in_(branch_ids)]
        if date_filters:
            qr_filters.extend(date_filters)
        
        social_filters_nvr = [SocialClick.branch_id.in_(branch_ids)]
        if date_filters:
            social_filters_nvr.extend([
                SocialClick.clicked_at >= date_filters[0].right if len(date_filters) > 0 else None,
                SocialClick.clicked_at <= date_filters[1].right if len(date_filters) > 1 else None
            ])
            social_filters_nvr = [f for f in social_filters_nvr if f is not None]
        
        new_vs_returning = await calculate_new_vs_returning(
            db, qr_filters, social_filters_nvr
        )
        
        region_analytics = RegionAnalytics(
            region_id=region.id,
            region_name=region.name,
            total_qr_scans=total_qr_scans,
            total_social_clicks=total_social_clicks,
            combined_total=total_qr_scans + total_social_clicks,
            new_vs_returning=new_vs_returning,
            clusters=[]
        )
        
        # Include cluster details if requested
        if include_details:
            clusters_result = await db.execute(
                select(Cluster).where(
                    Cluster.region_id == region.id,
                    Cluster.is_active == True
                ).order_by(Cluster.name)
            )
            clusters = clusters_result.scalars().all()
            
            for cluster in clusters:
                cluster_analytics = await get_cluster_analytics_internal(
                    db, cluster, start_date, end_date, include_branches=True
                )
                region_analytics.clusters.append(cluster_analytics)
        
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
    
    # Build date filters
    date_filters = []
    if start_date:
        try:
            start_dt = datetime.fromisoformat(start_date)
            date_filters.append(QRScan.scanned_at >= start_dt)
        except ValueError:
            pass
    
    if end_date:
        try:
            end_dt = datetime.fromisoformat(end_date) + timedelta(days=1) - timedelta(seconds=1)
            date_filters.append(QRScan.scanned_at <= end_dt)
        except ValueError:
            pass
    
    # Get all branches in this cluster
    branch_ids_query = select(Branch.id).where(
        Branch.cluster_id == cluster.id,
        Branch.is_active == True
    )
    branch_ids_result = await db.execute(branch_ids_query)
    branch_ids = [row[0] for row in branch_ids_result.all()]
    
    # QR Scans
    qr_query = select(func.count(QRScan.id)).join(QRCode).where(
        QRCode.branch_id.in_(branch_ids)
    )
    if date_filters:
        qr_query = qr_query.where(and_(*date_filters))
    
    total_qr_scans = (await db.execute(qr_query)).scalar() or 0
    
    # Social Clicks
    social_query = select(func.count(SocialClick.id)).where(
        SocialClick.branch_id.in_(branch_ids)
    )
    if date_filters:
        social_filters = []
        if start_date:
            social_filters.append(SocialClick.clicked_at >= datetime.fromisoformat(start_date))
        if end_date:
            social_filters.append(SocialClick.clicked_at <= datetime.fromisoformat(end_date) + timedelta(days=1))
        if social_filters:
            social_query = social_query.where(and_(*social_filters))
    
    total_social_clicks = (await db.execute(social_query)).scalar() or 0
    
    # New vs Returning
    qr_filters = [QRCode.branch_id.in_(branch_ids)]
    if date_filters:
        qr_filters.extend(date_filters)
    
    social_filters_nvr = [SocialClick.branch_id.in_(branch_ids)]
    if date_filters:
        if start_date:
            social_filters_nvr.append(SocialClick.clicked_at >= datetime.fromisoformat(start_date))
        if end_date:
            social_filters_nvr.append(SocialClick.clicked_at <= datetime.fromisoformat(end_date) + timedelta(days=1))
    
    new_vs_returning = await calculate_new_vs_returning(
        db, qr_filters, social_filters_nvr
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
    
    # Build date filters
    date_filters = []
    if start_date:
        try:
            start_dt = datetime.fromisoformat(start_date)
            date_filters.append(QRScan.scanned_at >= start_dt)
        except ValueError:
            pass
    
    if end_date:
        try:
            end_dt = datetime.fromisoformat(end_date) + timedelta(days=1) - timedelta(seconds=1)
            date_filters.append(QRScan.scanned_at <= end_dt)
        except ValueError:
            pass
    
    # QR Scans
    qr_query = select(func.count(QRScan.id)).join(QRCode).where(
        QRCode.branch_id == branch.id
    )
    if date_filters:
        qr_query = qr_query.where(and_(*date_filters))
    
    total_qr_scans = (await db.execute(qr_query)).scalar() or 0
    
    # Social Clicks
    social_query = select(func.count(SocialClick.id)).where(
        SocialClick.branch_id == branch.id
    )
    if date_filters:
        social_filters = []
        if start_date:
            social_filters.append(SocialClick.clicked_at >= datetime.fromisoformat(start_date))
        if end_date:
            social_filters.append(SocialClick.clicked_at <= datetime.fromisoformat(end_date) + timedelta(days=1))
        if social_filters:
            social_query = social_query.where(and_(*social_filters))
    
    total_social_clicks = (await db.execute(social_query)).scalar() or 0
    
    # New vs Returning
    qr_filters = [QRCode.branch_id == branch.id]
    if date_filters:
        qr_filters.extend(date_filters)
    
    social_filters_nvr = [SocialClick.branch_id == branch.id]
    if date_filters:
        if start_date:
            social_filters_nvr.append(SocialClick.clicked_at >= datetime.fromisoformat(start_date))
        if end_date:
            social_filters_nvr.append(SocialClick.clicked_at <= datetime.fromisoformat(end_date) + timedelta(days=1))
    
    new_vs_returning = await calculate_new_vs_returning(
        db, qr_filters, social_filters_nvr
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
    
    # Build filters
    filters = []
    
    # Hierarchical filtering
    if branch_id:
        filters.append(SocialClick.branch_id == branch_id)
    elif cluster_id:
        # Get all branches in cluster
        branch_ids_query = select(Branch.id).where(Branch.cluster_id == cluster_id)
        branch_ids_result = await db.execute(branch_ids_query)
        branch_ids = [row[0] for row in branch_ids_result.all()]
        filters.append(SocialClick.branch_id.in_(branch_ids))
    elif region_id:
        # Get all branches in region
        branch_ids_query = select(Branch.id).join(Cluster).where(Cluster.region_id == region_id)
        branch_ids_result = await db.execute(branch_ids_query)
        branch_ids = [row[0] for row in branch_ids_result.all()]
        filters.append(SocialClick.branch_id.in_(branch_ids))
    
    # Date filters
    if start_date:
        try:
            filters.append(SocialClick.clicked_at >= datetime.fromisoformat(start_date))
        except ValueError:
            pass
    
    if end_date:
        try:
            end_dt = datetime.fromisoformat(end_date) + timedelta(days=1) - timedelta(seconds=1)
            filters.append(SocialClick.clicked_at <= end_dt)
        except ValueError:
            pass
    
    # Total clicks
    total_query = select(func.count(SocialClick.id))
    if filters:
        total_query = total_query.where(and_(*filters))
    
    total_clicks = (await db.execute(total_query)).scalar() or 0
    
    # Platform breakdown
    platform_query = select(
        SocialClick.platform,
        func.count(SocialClick.id).label('count')
    ).group_by(SocialClick.platform).order_by(func.count(SocialClick.id).desc())
    
    if filters:
        platform_query = platform_query.where(and_(*filters))
    
    platform_result = await db.execute(platform_query)
    platform_breakdown = [
        {"platform": row.platform, "count": row.count}
        for row in platform_result.all()
    ]
    
    # New vs Returning
    new_vs_returning = await calculate_new_vs_returning(
        db, qr_filters=None, social_filters=filters if filters else None
    )
    
    return SocialAnalytics(
        total_clicks=total_clicks,
        new_vs_returning=new_vs_returning,
        platform_breakdown=platform_breakdown,
        region_id=region_id,
        cluster_id=cluster_id,
        branch_id=branch_id
    )
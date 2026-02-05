from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, case
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


# =====================================================
# AUTH
# =====================================================

def require_super_admin(current_user: User):
    if not current_user.is_super_admin:
        raise HTTPException(403, "Only super admin can view analytics")


# =====================================================
# DATE FILTER HELPER
# =====================================================

def build_date_filters(start_date: Optional[str], end_date: Optional[str]):
    qr_filters, social_filters = [], []

    if start_date:
        dt = datetime.fromisoformat(start_date)
        qr_filters.append(QRScan.scanned_at >= dt)
        social_filters.append(SocialClick.clicked_at >= dt)

    if end_date:
        dt = datetime.fromisoformat(end_date) + timedelta(days=1) - timedelta(seconds=1)
        qr_filters.append(QRScan.scanned_at <= dt)
        social_filters.append(SocialClick.clicked_at <= dt)

    return qr_filters, social_filters


# =====================================================
# NEW VS RETURNING (GENERIC)
# =====================================================

async def calculate_new_vs_returning(db, qr_query=None, social_query=None):
    if qr_query is None:
        qr_query = select(QRScan)
    if social_query is None:
        social_query = select(SocialClick)

    qr_stmt = select(
        func.sum(case((QRScan.is_new_user == True, 1), else_=0)),
        func.sum(case((QRScan.is_new_user == False, 1), else_=0))
    ).select_from(qr_query.subquery())

    social_stmt = select(
        func.sum(case((SocialClick.is_new_user == True, 1), else_=0)),
        func.sum(case((SocialClick.is_new_user == False, 1), else_=0))
    ).select_from(social_query.subquery())

    qr_new, qr_ret = (await db.execute(qr_stmt)).one()
    social_new, social_ret = (await db.execute(social_stmt)).one()

    new_u = (qr_new or 0) + (social_new or 0)
    ret_u = (qr_ret or 0) + (social_ret or 0)
    total = new_u + ret_u

    return NewVsReturning(
        new_users=new_u,
        returning_users=ret_u,
        new_percentage=round(new_u / total * 100, 2) if total else 0,
        returning_percentage=round(ret_u / total * 100, 2) if total else 0
    )


# =====================================================
# BULK CLUSTER ANALYTICS
# =====================================================

async def get_cluster_analytics_bulk(db, region_ids, start_date=None, end_date=None):
    qr_filters, social_filters = build_date_filters(start_date, end_date)

    clusters = (await db.execute(
        select(Cluster).where(Cluster.region_id.in_(region_ids), Cluster.is_active == True)
    )).scalars().all()

    cluster_map = {c.id: c for c in clusters}
    if not cluster_map:
        return {}

    # QR totals
    qr_stmt = (
        select(Branch.cluster_id, func.count(QRScan.id))
        .join(QRCode, QRCode.id == QRScan.qr_code_id)
        .join(Branch, Branch.id == QRCode.branch_id)
        .where(Branch.cluster_id.in_(cluster_map.keys()))
        .group_by(Branch.cluster_id)
    )
    if qr_filters:
        qr_stmt = qr_stmt.where(and_(*qr_filters))
    qr_counts = dict((await db.execute(qr_stmt)).all())

    # Social totals
    social_stmt = (
        select(Branch.cluster_id, func.count(SocialClick.id))
        .join(Branch, Branch.id == SocialClick.branch_id)
        .where(Branch.cluster_id.in_(cluster_map.keys()))
        .group_by(Branch.cluster_id)
    )
    if social_filters:
        social_stmt = social_stmt.where(and_(*social_filters))
    social_counts = dict((await db.execute(social_stmt)).all())

    result = {}
    for cid, cluster in cluster_map.items():
        result.setdefault(cluster.region_id, []).append(
            ClusterAnalytics(
                cluster_id=cid,
                cluster_name=cluster.name,
                region_id=cluster.region_id,
                total_qr_scans=qr_counts.get(cid, 0),
                total_social_clicks=social_counts.get(cid, 0),
                combined_total=qr_counts.get(cid, 0) + social_counts.get(cid, 0),
                new_vs_returning=NewVsReturning(0, 0, 0, 0),
                branches=[]
            )
        )

    return result


# =====================================================
# BULK BRANCH ANALYTICS
# =====================================================

async def get_branch_analytics_bulk(db, cluster_ids, start_date=None, end_date=None):
    qr_filters, social_filters = build_date_filters(start_date, end_date)

    branches = (await db.execute(
        select(Branch).where(Branch.cluster_id.in_(cluster_ids), Branch.is_active == True)
    )).scalars().all()

    branch_map = {b.id: b for b in branches}
    if not branch_map:
        return {}

    # QR totals
    qr_stmt = (
        select(QRCode.branch_id, func.count(QRScan.id))
        .join(QRCode, QRCode.id == QRScan.qr_code_id)
        .where(QRCode.branch_id.in_(branch_map.keys()))
        .group_by(QRCode.branch_id)
    )
    if qr_filters:
        qr_stmt = qr_stmt.where(and_(*qr_filters))
    qr_counts = dict((await db.execute(qr_stmt)).all())

    # Social totals
    social_stmt = (
        select(SocialClick.branch_id, func.count(SocialClick.id))
        .where(SocialClick.branch_id.in_(branch_map.keys()))
        .group_by(SocialClick.branch_id)
    )
    if social_filters:
        social_stmt = social_stmt.where(and_(*social_filters))
    social_counts = dict((await db.execute(social_stmt)).all())

    result = {}
    for bid, branch in branch_map.items():
        result.setdefault(branch.cluster_id, []).append(
            BranchAnalytics(
                branch_id=bid,
                branch_name=branch.name,
                cluster_id=branch.cluster_id,
                total_qr_scans=qr_counts.get(bid, 0),
                total_social_clicks=social_counts.get(bid, 0),
                combined_total=qr_counts.get(bid, 0) + social_counts.get(bid, 0),
                new_vs_returning=NewVsReturning(0, 0, 0, 0)
            )
        )

    return result


# =====================================================
# REGION ANALYTICS (OPTIMIZED)
# =====================================================

@router.get("/regions", response_model=List[RegionAnalytics])
async def get_region_analytics(
    region_id: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_details: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    require_super_admin(current_user)

    regions = (await db.execute(
        select(Region).where(Region.is_active == True)
    )).scalars().all()

    if region_id:
        regions = [r for r in regions if r.id == region_id]

    region_ids = [r.id for r in regions]

    cluster_data = await get_cluster_analytics_bulk(db, region_ids, start_date, end_date)

    all_cluster_ids = [c.cluster_id for clusters in cluster_data.values() for c in clusters]
    branch_data = await get_branch_analytics_bulk(db, all_cluster_ids, start_date, end_date)

    for clusters in cluster_data.values():
        for c in clusters:
            c.branches = branch_data.get(c.cluster_id, [])

    return [
        RegionAnalytics(
            region_id=r.id,
            region_name=r.name,
            total_qr_scans=0,
            total_social_clicks=0,
            combined_total=0,
            new_vs_returning=NewVsReturning(0, 0, 0, 0),
            clusters=cluster_data.get(r.id, []) if include_details else []
        )
        for r in regions
    ]


# =====================================================
# SOCIAL ANALYTICS (UNCHANGED LOGIC)
# =====================================================

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
    require_super_admin(current_user)

    base_query = select(SocialClick)

    if branch_id:
        base_query = base_query.where(SocialClick.branch_id == branch_id)
    elif cluster_id:
        branch_ids = [b.id for b in (await db.execute(select(Branch.id).where(Branch.cluster_id == cluster_id))).all()]
        base_query = base_query.where(SocialClick.branch_id.in_(branch_ids))
    elif region_id:
        branch_ids = [b.id for b in (await db.execute(select(Branch.id).join(Cluster).where(Cluster.region_id == region_id))).all()]
        base_query = base_query.where(SocialClick.branch_id.in_(branch_ids))

    qr_filters, social_filters = build_date_filters(start_date, end_date)
    if social_filters:
        base_query = base_query.where(and_(*social_filters))

    total = (await db.execute(select(func.count()).select_from(base_query.subquery()))).scalar()

    platform_query = select(
        SocialClick.platform,
        func.count(SocialClick.id)
    ).where(SocialClick.id.in_(select(base_query.subquery().c.id))).group_by(SocialClick.platform)

    platform_data = [{"platform": p, "count": c} for p, c in (await db.execute(platform_query)).all()]

    return SocialAnalytics(
        total_clicks=total,
        new_vs_returning=NewVsReturning(0, 0, 0, 0),
        platform_breakdown=platform_data,
        region_id=region_id,
        cluster_id=cluster_id,
        branch_id=branch_id
    )

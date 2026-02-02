from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List

from database import get_db
from schemas import (
    RegionCreate, RegionResponse,
    ClusterCreate, ClusterResponse,
    BranchCreate, BranchResponse
)
from auth import get_current_user
from models import User, Region, Cluster, Branch

router = APIRouter(prefix="/api", tags=["Hierarchy Management"])


def require_super_admin(current_user: User):
    """Helper to check if user is super admin"""
    if not current_user.is_super_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only super admin can perform this action"
        )


# ============================================
# REGION ROUTES
# ============================================
@router.get("/regions", response_model=List[RegionResponse])
async def get_all_regions(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get all regions"""
    result = await db.execute(
        select(Region).where(Region.is_active == True).order_by(Region.name)
    )
    return result.scalars().all()


@router.post("/regions", response_model=RegionResponse, status_code=status.HTTP_201_CREATED)
async def create_region(
    region: RegionCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create new region (Super Admin only)"""
    require_super_admin(current_user)
    
    # Check if already exists
    result = await db.execute(
        select(Region).where(Region.name == region.name)
    )
    if result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Region name already exists"
        )
    
    new_region = Region(name=region.name, code=region.code)
    db.add(new_region)
    await db.commit()
    await db.refresh(new_region)
    return new_region


# ============================================
# CLUSTER ROUTES
# ============================================
@router.get("/clusters", response_model=List[ClusterResponse])
async def get_all_clusters(
    region_id: int = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get all clusters, optionally filtered by region"""
    query = select(Cluster).where(Cluster.is_active == True)
    
    if region_id:
        query = query.where(Cluster.region_id == region_id)
    
    query = query.order_by(Cluster.name)
    result = await db.execute(query)
    return result.scalars().all()


@router.post("/clusters", response_model=ClusterResponse, status_code=status.HTTP_201_CREATED)
async def create_cluster(
    cluster: ClusterCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create new cluster (Super Admin only)"""
    require_super_admin(current_user)
    
    # Verify region exists
    result = await db.execute(
        select(Region).where(Region.id == cluster.region_id)
    )
    if not result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Region not found"
        )
    
    new_cluster = Cluster(
        name=cluster.name,
        code=cluster.code,
        region_id=cluster.region_id
    )
    db.add(new_cluster)
    await db.commit()
    await db.refresh(new_cluster)
    return new_cluster


# ============================================
# BRANCH ROUTES
# ============================================
@router.get("/branches", response_model=List[BranchResponse])
async def get_all_branches(
    region_id: int = None,
    cluster_id: int = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get all branches, optionally filtered by region or cluster"""
    query = select(Branch).where(Branch.is_active == True)
    
    if cluster_id:
        query = query.where(Branch.cluster_id == cluster_id)
    elif region_id:
        # Get branches through cluster
        query = query.join(Cluster).where(Cluster.region_id == region_id)
    
    query = query.order_by(Branch.name)
    result = await db.execute(query)
    return result.scalars().all()


@router.post("/branches", response_model=BranchResponse, status_code=status.HTTP_201_CREATED)
async def create_branch(
    branch: BranchCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create new branch (Super Admin only)"""
    require_super_admin(current_user)
    
    # Verify cluster exists
    result = await db.execute(
        select(Cluster).where(Cluster.id == branch.cluster_id)
    )
    if not result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cluster not found"
        )
    
    new_branch = Branch(
        name=branch.name,
        code=branch.code,
        location=branch.location,
        cluster_id=branch.cluster_id
    )
    db.add(new_branch)
    await db.commit()
    await db.refresh(new_branch)
    return new_branch


@router.patch("/branches/{branch_id}", response_model=BranchResponse)
async def update_branch(
    branch_id: int,
    branch_update: BranchCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update branch (Super Admin only)"""
    require_super_admin(current_user)
    
    result = await db.execute(
        select(Branch).where(Branch.id == branch_id)
    )
    branch = result.scalar_one_or_none()
    
    if not branch:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Branch not found"
        )
    
    branch.name = branch_update.name
    branch.code = branch_update.code
    branch.location = branch_update.location
    branch.cluster_id = branch_update.cluster_id
    
    await db.commit()
    await db.refresh(branch)
    return branch


@router.delete("/branches/{branch_id}")
async def deactivate_branch(
    branch_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Deactivate branch (Super Admin only)"""
    require_super_admin(current_user)
    
    result = await db.execute(
        select(Branch).where(Branch.id == branch_id)
    )
    branch = result.scalar_one_or_none()
    
    if not branch:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Branch not found"
        )
    
    branch.is_active = False
    await db.commit()
    
    return {"message": f"Branch '{branch.name}' deactivated successfully"}


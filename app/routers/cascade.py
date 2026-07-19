from fastapi import APIRouter
from typing import List
from app.schemas.cascade import CascadePrediction
from engine.cascade_predictor import predict_cascades

router = APIRouter()


@router.get("/cascade", response_model=List[CascadePrediction])
def get_cascade_predictions():
    """
    Returns at-risk services (WORSENING trend, near breach) along with
    their predicted downstream impact, traced through the dependency graph.
    """
    return predict_cascades()
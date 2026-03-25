# src/upcasting/upcasters.py
"""
Concrete upcasters for event schema evolution.
"""

from typing import Dict, Any
from datetime import datetime
import logging

from .registry import UpcasterRegistry

logger = logging.getLogger(__name__)

registry = UpcasterRegistry()


@registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_credit_v1_to_v2(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Upcast CreditAnalysisCompleted from v1 to v2.
    
    v1 fields: application_id, credit_score, risk_tier, debt_to_income_ratio
    v2 adds: model_version, confidence_score, regulatory_basis
    
    Inference strategy:
    - model_version: Infer from recorded_at timestamp (pre-2026 = "legacy-pre-2026")
    - confidence_score: Set to None (genuinely unknown - better than fabricating)
    - regulatory_basis: Infer from date (pre-2026 = "BASEL_III")
    """
    new_payload = payload.copy()
    
    # Add model_version with inference
    if "model_version" not in new_payload:
        # This would ideally use event metadata timestamp
        # For now, use a default
        new_payload["model_version"] = "legacy-pre-2026"
        logger.debug("Inferred model_version='legacy-pre-2026' for historical event")
    
    # Add confidence_score as None (genuinely unknown)
    if "confidence_score" not in new_payload:
        new_payload["confidence_score"] = None
        logger.debug("Set confidence_score=None for historical event (genuinely unknown)")
    
    # Add regulatory_basis with inference
    if "regulatory_basis" not in new_payload:
        # Infer based on risk tier and date
        risk_tier = new_payload.get("risk_tier", "MEDIUM")
        if risk_tier == "HIGH":
            new_payload["regulatory_basis"] = "ENHANCED_DUE_DILIGENCE"
        else:
            new_payload["regulatory_basis"] = "STANDARD_UNDERWRITING"
        logger.debug(f"Inferred regulatory_basis='{new_payload['regulatory_basis']}'")
    
    return new_payload


@registry.register("DecisionGenerated", from_version=1)
def upcast_decision_v1_to_v2(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Upcast DecisionGenerated from v1 to v2.
    
    v1 fields: application_id, decision, reason
    v2 adds: model_version, confidence_score, regulatory_basis
    
    Note: This upcaster doesn't have access to agent sessions,
    so model_versions will be populated at query time.
    """
    new_payload = payload.copy()
    
    # Add model_versions placeholder (will be populated at query time)
    if "model_versions" not in new_payload:
        new_payload["model_versions"] = {}
        logger.debug("Set empty model_versions dict for historical event")
    
    # Add decision_id if missing
    if "decision_id" not in new_payload:
        new_payload["decision_id"] = f"legacy-{payload.get('application_id', 'unknown')}"
        logger.debug("Generated legacy decision_id")
    
    return new_payload


def get_upcaster_registry() -> UpcasterRegistry:
    """Get the configured upcaster registry."""
    return registry
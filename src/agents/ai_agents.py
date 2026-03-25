# src/agents/ai_agents.py
"""
AI Agent implementations using LLM for decision making
Integrates with The Ledger event sourcing system
"""

import asyncio
import json
import uuid
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime

from src.models.llm_config import get_llm_client, LLMClient
from src.event_store import EventStore
from src.commands.handlers import CommandHandlers
from src.models.events import (
    ApplicationSubmitted, CreditAnalysisCompleted, 
    FraudScreeningCompleted, ComplianceRulePassed, DecisionGenerated
)


@dataclass
class AgentContext:
    """Context for AI agent"""
    agent_id: str
    session_id: str
    agent_type: str
    model_version: str
    application_id: Optional[str] = None
    previous_decisions: List[Dict] = field(default_factory=list)
    current_state: Dict[str, Any] = field(default_factory=dict)


class AIAgent:
    """Base AI Agent with LLM integration"""
    
    def __init__(
        self,
        agent_id: str,
        agent_type: str,
        model_version: str,
        event_store: EventStore,
        command_handlers: CommandHandlers,
        llm_client: Optional[LLMClient] = None
    ):
        self.agent_id = agent_id
        self.agent_type = agent_type
        self.model_version = model_version
        self.event_store = event_store
        self.command_handlers = command_handlers
        self.llm_client = llm_client or get_llm_client()
        self.session_id = f"{agent_id}-{uuid.uuid4().hex[:8]}"
        self.context: Optional[AgentContext] = None
    
    async def start_session(self, context_source: str = "database") -> None:
        """Start agent session with Gas Town pattern"""
        await self.command_handlers.handle_start_agent_session(
            agent_id=self.agent_id,
            session_id=self.session_id,
            context_source=context_source,
            model_version=self.model_version,
            token_count=0,
            context_hash=f"session-{self.session_id}"
        )
        
        self.context = AgentContext(
            agent_id=self.agent_id,
            session_id=self.session_id,
            agent_type=self.agent_type,
            model_version=self.model_version
        )
        
        print(f"✓ Agent {self.agent_id} started session {self.session_id}")
    
    async def process_application(self, application_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process an application - to be implemented by subclasses"""
        raise NotImplementedError


class CreditAnalystAgent(AIAgent):
    """Credit analyst AI agent"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, agent_type="credit_analyst", **kwargs)
    
    async def process_application(self, application_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze credit risk using LLM"""
        
        # Build prompt for LLM
        prompt = f"""
        Analyze the following loan application and provide credit risk assessment:
        
        Application ID: {application_id}
        Applicant ID: {data.get('applicant_id')}
        Requested Amount: ${data.get('requested_amount'):,.2f}
        Business Name: {data.get('business_name')}
        Tax ID: {data.get('tax_id')}
        
        Provide analysis including:
        1. Risk tier (LOW, MEDIUM, HIGH)
        2. Credit score (0-1000)
        3. Maximum credit limit
        4. Confidence score (0-1)
        5. Reasoning
        
        Respond with JSON only.
        """
        
        schema = {
            "type": "object",
            "properties": {
                "risk_tier": {"type": "string", "enum": ["LOW", "MEDIUM", "HIGH"]},
                "credit_score": {"type": "integer", "minimum": 0, "maximum": 1000},
                "max_credit_limit": {"type": "number", "minimum": 0},
                "confidence_score": {"type": "number", "minimum": 0, "maximum": 1},
                "reasoning": {"type": "string"}
            },
            "required": ["risk_tier", "credit_score", "max_credit_limit", "confidence_score"]
        }
        
        # Get LLM decision
        result = await self.llm_client.generate_structured(
            prompt,
            schema,
            agent_type="credit_analyst"
        )
        
        # Record the decision in event store
        await self.command_handlers.handle_credit_analysis_completed(
            application_id=application_id,
            agent_id=self.agent_id,
            session_id=self.session_id,
            risk_tier=result["risk_tier"],
            credit_score=result["credit_score"],
            max_credit_limit=result["max_credit_limit"],
            model_version=self.model_version,
            confidence_score=result["confidence_score"]
        )
        
        return result


class FraudDetectorAgent(AIAgent):
    """Fraud detection AI agent"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, agent_type="fraud_detector", **kwargs)
    
    async def process_application(self, application_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Detect fraud using LLM"""
        
        prompt = f"""
        Analyze the following application for potential fraud:
        
        Application ID: {application_id}
        Applicant ID: {data.get('applicant_id')}
        Requested Amount: ${data.get('requested_amount'):,.2f}
        Business Name: {data.get('business_name')}
        Tax ID: {data.get('tax_id')}
        
        Provide fraud assessment including:
        1. Fraud score (0-1, higher = more suspicious)
        2. Flags (list of suspicious indicators)
        3. Risk indicators (additional details)
        4. Confidence score (0-1)
        
        Respond with JSON only.
        """
        
        schema = {
            "type": "object",
            "properties": {
                "fraud_score": {"type": "number", "minimum": 0, "maximum": 1},
                "flags": {"type": "array", "items": {"type": "string"}},
                "risk_indicators": {"type": "object"},
                "confidence_score": {"type": "number", "minimum": 0, "maximum": 1}
            },
            "required": ["fraud_score", "flags"]
        }
        
        result = await self.llm_client.generate_structured(
            prompt,
            schema,
            agent_type="fraud_detector"
        )
        
        # Record fraud screening
        from src.models.events import FraudScreeningCompleted
        event = FraudScreeningCompleted(
            application_id=application_id,
            fraud_score=result["fraud_score"],
            flags=result["flags"],
            risk_indicators=result.get("risk_indicators", {})
        )
        
        stream_id = f"loan-{application_id}"
        version = await self.event_store.stream_version(stream_id)
        await self.event_store.append(stream_id, [event], expected_version=version)
        
        return result


class ComplianceAgent(AIAgent):
    """Compliance verification AI agent"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, agent_type="compliance_officer", **kwargs)
    
    async def process_application(self, application_id: str, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Verify compliance using LLM"""
        
        prompt = f"""
        Verify compliance for the following application:
        
        Application ID: {application_id}
        Applicant ID: {data.get('applicant_id')}
        Requested Amount: ${data.get('requested_amount'):,.2f}
        Business Name: {data.get('business_name')}
        Tax ID: {data.get('tax_id')}
        
        Check against regulations:
        - BASEL_III_2024 (capital requirements)
        - KYC_VERIFIED (customer identification)
        - AML_CHECK (anti-money laundering)
        - SANCTIONS_SCREENING (sanctions list)
        - CREDIT_CAPACITY (ability to repay)
        
        For each regulation, determine if the application passes.
        Respond with JSON array of results.
        """
        
        schema = {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "rule_id": {"type": "string"},
                    "passed": {"type": "boolean"},
                    "failure_reason": {"type": "string"}
                },
                "required": ["rule_id", "passed"]
            }
        }
        
        results = await self.llm_client.generate_structured(
            prompt,
            schema,
            agent_type="compliance_officer"
        )
        
        # Record compliance checks
        from src.models.events import ComplianceRulePassed, ComplianceRuleFailed
        
        for result in results:
            if result["passed"]:
                event = ComplianceRulePassed(
                    application_id=application_id,
                    rule_id=result["rule_id"],
                    regulation_version="v2.0",
                    check_id=f"check-{uuid.uuid4().hex[:8]}",
                    details={"verified_by": self.agent_id}
                )
            else:
                event = ComplianceRuleFailed(
                    application_id=application_id,
                    rule_id=result["rule_id"],
                    regulation_version="v2.0",
                    check_id=f"check-{uuid.uuid4().hex[:8]}",
                    failure_reason=result.get("failure_reason", "Compliance check failed")
                )
            
            stream_id = f"loan-{application_id}"
            version = await self.event_store.stream_version(stream_id)
            await self.event_store.append(stream_id, [event], expected_version=version)
        
        return results


class DecisionOrchestratorAgent(AIAgent):
    """Decision orchestrator AI agent"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, agent_type="decision_orchestrator", **kwargs)
    
    async def process_application(
        self,
        application_id: str,
        credit_analysis: Dict[str, Any],
        fraud_screening: Dict[str, Any],
        compliance_checks: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Synthesize decisions using LLM"""
        
        prompt = f"""
        Synthesize the following analysis results into a final decision:
        
        Application ID: {application_id}
        
        Credit Analysis:
        - Risk Tier: {credit_analysis.get('risk_tier')}
        - Credit Score: {credit_analysis.get('credit_score')}
        - Confidence: {credit_analysis.get('confidence_score')}
        
        Fraud Screening:
        - Fraud Score: {fraud_screening.get('fraud_score')}
        - Flags: {fraud_screening.get('flags', [])}
        
        Compliance Checks:
        {json.dumps(compliance_checks, indent=2)}
        
        Provide final recommendation (APPROVE, DECLINE, or REFER) with reasoning.
        Respond with JSON.
        """
        
        schema = {
            "type": "object",
            "properties": {
                "recommendation": {"type": "string", "enum": ["APPROVE", "DECLINE", "REFER"]},
                "confidence_score": {"type": "number", "minimum": 0, "maximum": 1},
                "reasoning": {"type": "string"}
            },
            "required": ["recommendation", "confidence_score", "reasoning"]
        }
        
        result = await self.llm_client.generate_structured(
            prompt,
            schema,
            agent_type="decision_orchestrator"
        )
        
        # Record decision
        await self.command_handlers.handle_generate_decision(
            application_id=application_id,
            agent_id=self.agent_id,
            session_id=self.session_id,
            orchestrator_recommendation=result["recommendation"],
            orchestrator_confidence=result["confidence_score"],
            contributing_sessions=[self.session_id],
            reasoning=result["reasoning"]
        )
        
        return result


class AgentOrchestrator:
    """Orchestrates multiple AI agents for complete application processing"""
    
    def __init__(
        self,
        event_store: EventStore,
        command_handlers: CommandHandlers
    ):
        self.event_store = event_store
        self.command_handlers = command_handlers
        self.agents: Dict[str, AIAgent] = {}
    
    async def create_agent(
        self,
        agent_type: str,
        model_version: str = "v2.0"
    ) -> AIAgent:
        """Create and initialize an AI agent"""
        agent_id = f"{agent_type}-{uuid.uuid4().hex[:8]}"
        
        if agent_type == "credit_analyst":
            agent = CreditAnalystAgent(
                agent_id, agent_type, model_version,
                self.event_store, self.command_handlers
            )
        elif agent_type == "fraud_detector":
            agent = FraudDetectorAgent(
                agent_id, agent_type, model_version,
                self.event_store, self.command_handlers
            )
        elif agent_type == "compliance_officer":
            agent = ComplianceAgent(
                agent_id, agent_type, model_version,
                self.event_store, self.command_handlers
            )
        elif agent_type == "decision_orchestrator":
            agent = DecisionOrchestratorAgent(
                agent_id, agent_type, model_version,
                self.event_store, self.command_handlers
            )
        else:
            raise ValueError(f"Unknown agent type: {agent_type}")
        
        await agent.start_session()
        self.agents[agent.agent_id] = agent
        
        return agent
    
    async def process_application(
        self,
        application_id: str,
        application_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process an application through all agents"""
        
        print(f"\n{'='*60}")
        print(f"Processing Application: {application_id}")
        print(f"{'='*60}")
        
        # Submit application first
        await self.command_handlers.handle_submit_application(
            application_id=application_id,
            applicant_id=application_data["applicant_id"],
            requested_amount=application_data["requested_amount"],
            business_name=application_data["business_name"],
            tax_id=application_data["tax_id"]
        )
        print(f"✓ Application submitted")
        
        # Create agents if not exist
        if "credit_analyst" not in [a.agent_type for a in self.agents.values()]:
            await self.create_agent("credit_analyst")
            await self.create_agent("fraud_detector")
            await self.create_agent("compliance_officer")
            await self.create_agent("decision_orchestrator")
        
        # Find agents
        credit_agent = next(a for a in self.agents.values() if a.agent_type == "credit_analyst")
        fraud_agent = next(a for a in self.agents.values() if a.agent_type == "fraud_detector")
        compliance_agent = next(a for a in self.agents.values() if a.agent_type == "compliance_officer")
        decision_agent = next(a for a in self.agents.values() if a.agent_type == "decision_orchestrator")
        
        # Run agents concurrently
        print("\n🤖 Running AI Agents...")
        
        credit_task = credit_agent.process_application(application_id, application_data)
        fraud_task = fraud_agent.process_application(application_id, application_data)
        compliance_task = compliance_agent.process_application(application_id, application_data)
        
        credit_result, fraud_result, compliance_results = await asyncio.gather(
            credit_task, fraud_task, compliance_task
        )
        
        print(f"  ✓ Credit analysis: {credit_result.get('risk_tier')} risk, {credit_result.get('credit_score')} score")
        print(f"  ✓ Fraud screening: score {fraud_result.get('fraud_score')}")
        print(f"  ✓ Compliance checks: {len([c for c in compliance_results if c.get('passed')])}/{len(compliance_results)} passed")
        
        # Final decision
        print("\n🎯 Making Final Decision...")
        decision = await decision_agent.process_application(
            application_id,
            credit_result,
            fraud_result,
            compliance_results
        )
        
        print(f"  ✓ Decision: {decision['recommendation']} (confidence: {decision['confidence_score']:.2%})")
        print(f"  ✓ Reasoning: {decision['reasoning'][:100]}...")
        
        return {
            "application_id": application_id,
            "credit_analysis": credit_result,
            "fraud_screening": fraud_result,
            "compliance_checks": compliance_results,
            "final_decision": decision
        }
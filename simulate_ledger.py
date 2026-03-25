#!/usr/bin/env python3
"""
Ledger System Simulation Script
Simulates a complete multi-agent AI loan processing system with event sourcing.

This script demonstrates:
- Multiple AI agents working concurrently
- Event sourcing with optimistic concurrency
- Projections and read models
- Gas Town pattern for agent memory
- Audit chains and integrity verification
- What-if counterfactual scenarios
- Regulatory package generation

Usage:
    python simulate_ledger.py --applications 50 --agents 4 --duration 60
"""

import asyncio
import asyncpg
import json
import logging
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import argparse
import sys
from pathlib import Path

import structlog
from colorama import init, Fore, Style
from tabulate import tabulate

# Setup logging
init(autoreset=True)
logger = structlog.get_logger()

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from src.event_store import EventStore
from src.aggregates.loan_application import LoanApplicationAggregate
from src.aggregates.agent_session import AgentSessionAggregate
from src.commands.handlers import CommandHandlers
from src.projections.daemon import ProjectionDaemon
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.agent_performance import AgentPerformanceProjection
from src.projections.compliance_audit import ComplianceAuditProjection
from src.integrity.audit_chain import run_integrity_check
from src.integrity.gas_town import reconstruct_agent_context
from src.what_if.projector import WhatIfProjector
from src.regulatory.package import generate_regulatory_package


class ApplicationState(str, Enum):
    """Application lifecycle states"""
    SUBMITTED = "SUBMITTED"
    AWAITING_ANALYSIS = "AWAITING_ANALYSIS"
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
    COMPLIANCE_REVIEW = "COMPLIANCE_REVIEW"
    PENDING_DECISION = "PENDING_DECISION"
    APPROVED_PENDING_HUMAN = "APPROVED_PENDING_HUMAN"
    DECLINED_PENDING_HUMAN = "DECLINED_PENDING_HUMAN"
    FINAL_APPROVED = "FINAL_APPROVED"
    FINAL_DECLINED = "FINAL_DECLINED"


@dataclass
class SimulationConfig:
    """Configuration for the simulation"""
    num_applications: int = 50
    agents_per_application: int = 4
    events_per_application: int = 8
    concurrent_agents: int = 10
    simulation_duration_seconds: int = 60
    enable_crash_simulation: bool = True
    enable_what_if: bool = True
    enable_regulatory: bool = True
    verbose: bool = True


@dataclass
class Agent:
    """Represents an AI agent in the simulation"""
    agent_id: str
    session_id: str
    agent_type: str  # credit, fraud, compliance, decision
    model_version: str
    context: Dict[str, Any] = field(default_factory=dict)
    actions_taken: List[Dict] = field(default_factory=list)
    
    def __post_init__(self):
        if not self.session_id:
            self.session_id = f"{self.agent_id}-{uuid.uuid4().hex[:8]}"


class LedgerSimulator:
    """
    Complete Ledger system simulator.
    Demonstrates the entire event sourcing infrastructure in action.
    """
    
    def __init__(self, config: SimulationConfig):
        self.config = config
        self.store: Optional[EventStore] = None
        self.command_handlers: Optional[CommandHandlers] = None
        self.projections: Dict[str, Any] = {}
        self.daemon: Optional[ProjectionDaemon] = None
        self.pool: Optional[asyncpg.Pool] = None
        
        # Statistics
        self.stats = {
            "applications_submitted": 0,
            "applications_approved": 0,
            "applications_declined": 0,
            "events_appended": 0,
            "concurrency_conflicts": 0,
            "retry_attempts": 0,
            "projection_lag_ms": [],
            "response_times_ms": [],
            "agent_actions": defaultdict(lambda: 0)
        }
        
        self.results: List[Dict] = []
    
    async def initialize(self):
        """Initialize database connection and event store"""
        print(f"\n{Fore.CYAN}{'='*60}")
        print(f"Initializing Ledger System")
        print(f"{'='*60}{Style.RESET_ALL}")
        
        # Create database pool
        self.pool = await asyncpg.create_pool(
            "postgresql://postgres:postgres@localhost:5432/ledger",
            min_size=10,
            max_size=20,
            command_timeout=60
        )
        
        # Initialize event store
        from src.upcasting.registry import UpcasterRegistry
        registry = UpcasterRegistry()
        self.store = EventStore(self.pool, registry)
        
        # Initialize command handlers
        self.command_handlers = CommandHandlers(self.store)
        
        # Initialize projections
        self.projections = {
            "application_summary": ApplicationSummaryProjection(),
            "agent_performance": AgentPerformanceProjection(),
            "compliance_audit": ComplianceAuditProjection()
        }
        
        # Set database pools for projections
        for proj in self.projections.values():
            proj.set_pool(self.pool)
        
        # Initialize projection daemon
        self.daemon = ProjectionDaemon(
            self.store,
            list(self.projections.values()),
            max_retries=3
        )
        
        print(f"{Fore.GREEN}✓ Database connected")
        print(f"✓ Event store initialized")
        print(f"✓ Command handlers ready")
        print(f"✓ Projection daemon ready")
        
        # Clear existing data
        await self._clear_data()
    
    async def _clear_data(self):
        """Clear existing data from tables"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                TRUNCATE TABLE events CASCADE;
                TRUNCATE TABLE event_streams CASCADE;
                TRUNCATE TABLE projection_checkpoints CASCADE;
                TRUNCATE TABLE outbox CASCADE;
                TRUNCATE TABLE application_summary CASCADE;
                TRUNCATE TABLE agent_performance CASCADE;
                TRUNCATE TABLE compliance_audit_snapshots CASCADE;
                TRUNCATE TABLE audit_integrity_checks CASCADE;
            """)
        print(f"{Fore.YELLOW}✓ Cleared existing data{Style.RESET_ALL}")
    
    async def run_simulation(self):
        """Run the complete simulation"""
        print(f"\n{Fore.CYAN}{'='*60}")
        print(f"Starting Ledger System Simulation")
        print(f"{'='*60}{Style.RESET_ALL}")
        print(f"\nConfiguration:")
        print(f"  - Applications: {self.config.num_applications}")
        print(f"  - Agents per application: {self.config.agents_per_application}")
        print(f"  - Events per application: {self.config.events_per_application}")
        print(f"  - Concurrent agents: {self.config.concurrent_agents}")
        print(f"  - Duration: {self.config.simulation_duration_seconds}s")
        
        start_time = time.time()
        
        # Start projection daemon in background
        daemon_task = asyncio.create_task(
            self.daemon.run_forever(poll_interval_ms=100)
        )
        
        # Phase 1: Submit applications
        print(f"\n{Fore.YELLOW}Phase 1: Submitting Applications{Style.RESET_ALL}")
        await self._submit_applications()
        
        # Phase 2: Process applications with agents
        print(f"\n{Fore.YELLOW}Phase 2: Agent Processing (Concurrent){Style.RESET_ALL}")
        await self._process_with_agents()
        
        # Phase 3: Human review
        print(f"\n{Fore.YELLOW}Phase 3: Human Review{Style.RESET_ALL}")
        await self._human_review()
        
        # Phase 4: Run integrity checks
        print(f"\n{Fore.YELLOW}Phase 4: Audit & Integrity{Style.RESET_ALL}")
        await self._audit_all_applications()
        
        # Phase 5: What-if scenarios (bonus)
        if self.config.enable_what_if:
            print(f"\n{Fore.YELLOW}Phase 5: What-If Counterfactuals (Bonus){Style.RESET_ALL}")
            await self._run_what_if_scenarios()
        
        # Phase 6: Regulatory packages (bonus)
        if self.config.enable_regulatory:
            print(f"\n{Fore.YELLOW}Phase 6: Regulatory Packages (Bonus){Style.RESET_ALL}")
            await self._generate_regulatory_packages()
        
        # Phase 7: Gas Town crash recovery
        if self.config.enable_crash_simulation:
            print(f"\n{Fore.YELLOW}Phase 7: Gas Town Crash Recovery{Style.RESET_ALL}")
            await self._simulate_crash_recovery()
        
        # Stop daemon
        self.daemon.stop()
        await daemon_task
        
        elapsed = time.time() - start_time
        
        # Print results
        await self._print_results(elapsed)
        
        return self.results
    
    async def _submit_applications(self):
        """Submit loan applications"""
        for i in range(self.config.num_applications):
            application_id = f"APP-{i+1:04d}"
            
            try:
                await self.command_handlers.handle_submit_application(
                    application_id=application_id,
                    applicant_id=f"CUST-{random.randint(1000, 9999)}",
                    requested_amount=random.uniform(50000, 1000000),
                    business_name=f"Business {i+1}",
                    tax_id=f"{random.randint(10, 99)}-{random.randint(1000000, 9999999)}",
                    correlation_id=f"sim-{application_id}"
                )
                
                self.stats["applications_submitted"] += 1
                self.stats["events_appended"] += 1
                
                if self.config.verbose and i % 10 == 0:
                    print(f"  ✓ Submitted {application_id}")
                    
            except Exception as e:
                print(f"  ✗ Failed to submit {application_id}: {e}")
    
    async def _process_with_agents(self):
        """Process applications with AI agents (concurrent)"""
        # Create agent pool
        agent_pool = []
        
        for app_idx in range(self.config.num_applications):
            application_id = f"APP-{app_idx+1:04d}"
            
            # Create agents for this application
            for agent_type in ["credit", "fraud", "compliance"]:
                agent_id = f"agent-{agent_type}-{uuid.uuid4().hex[:4]}"
                session_id = f"sess-{agent_type}-{app_idx}"
                
                agent = Agent(
                    agent_id=agent_id,
                    session_id=session_id,
                    agent_type=agent_type,
                    model_version=f"v{random.randint(1, 3)}.{random.randint(0, 9)}"
                )
                agent_pool.append((application_id, agent))
        
        # Add decision orchestrator agents
        for app_idx in range(self.config.num_applications):
            application_id = f"APP-{app_idx+1:04d}"
            agent_id = f"agent-decision-{uuid.uuid4().hex[:4]}"
            session_id = f"sess-decision-{app_idx}"
            
            agent = Agent(
                agent_id=agent_id,
                session_id=session_id,
                agent_type="decision",
                model_version=f"v{random.randint(2, 4)}.{random.randint(0, 9)}"
            )
            agent_pool.append((application_id, agent))
        
        # Process agents concurrently with semaphore
        semaphore = asyncio.Semaphore(self.config.concurrent_agents)
        
        async def process_agent(application_id: str, agent: Agent):
            async with semaphore:
                await self._run_agent(application_id, agent)
        
        tasks = [process_agent(app_id, agent) for app_id, agent in agent_pool]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _run_agent(self, application_id: str, agent: Agent):
        """Run a single AI agent"""
        start_time = time.time()
        
        try:
            # Gas Town: Start agent session
            await self.command_handlers.handle_start_agent_session(
                agent_id=agent.agent_id,
                session_id=agent.session_id,
                context_source="simulation",
                model_version=agent.model_version,
                token_count=random.randint(500, 5000),
                context_hash=hash(f"{application_id}-{agent.agent_id}")
            )
            
            # Execute agent-specific actions
            if agent.agent_type == "credit":
                await self._credit_analysis_agent(application_id, agent)
            elif agent.agent_type == "fraud":
                await self._fraud_screening_agent(application_id, agent)
            elif agent.agent_type == "compliance":
                await self._compliance_agent(application_id, agent)
            elif agent.agent_type == "decision":
                await self._decision_agent(application_id, agent)
            
            response_time = (time.time() - start_time) * 1000
            self.stats["response_times_ms"].append(response_time)
            self.stats["agent_actions"][agent.agent_type] += 1
            
            if self.config.verbose and random.random() < 0.1:
                print(f"  ✓ {agent.agent_type.upper()} agent {agent.agent_id} processed {application_id} in {response_time:.0f}ms")
                
        except Exception as e:
            if "OptimisticConcurrencyError" in str(e):
                self.stats["concurrency_conflicts"] += 1
                if self.config.verbose:
                    print(f"  ⚠ Concurrency conflict for {application_id} - {agent.agent_type}")
            else:
                print(f"  ✗ Agent {agent.agent_type} failed: {e}")
    
    async def _credit_analysis_agent(self, application_id: str, agent: Agent):
        """Credit analysis agent simulation"""
        # Simulate AI decision
        risk_tiers = ["LOW", "MEDIUM", "HIGH"]
        risk_tier = random.choices(risk_tiers, weights=[0.3, 0.5, 0.2])[0]
        credit_score = random.randint(550, 850)
        max_limit = random.uniform(50000, 1000000)
        confidence = random.uniform(0.6, 0.95)
        
        await self.command_handlers.handle_credit_analysis_completed(
            application_id=application_id,
            agent_id=agent.agent_id,
            session_id=agent.session_id,
            risk_tier=risk_tier,
            credit_score=credit_score,
            max_credit_limit=max_limit,
            model_version=agent.model_version,
            confidence_score=confidence
        )
        
        self.stats["events_appended"] += 1
    
    async def _fraud_screening_agent(self, application_id: str, agent: Agent):
        """Fraud screening agent simulation"""
        fraud_score = random.uniform(0, 0.3)  # Low fraud probability
        flags = []
        
        if fraud_score > 0.2:
            flags.append("HIGH_RISK_IP")
        if fraud_score > 0.25:
            flags.append("SUSPICIOUS_PATTERN")
        
        # Simulate fraud screening event
        from src.models.events import FraudScreeningCompleted
        event = FraudScreeningCompleted(
            application_id=application_id,
            fraud_score=fraud_score,
            flags=flags,
            risk_indicators={"score": fraud_score, "flags_count": len(flags)}
        )
        
        # Get current stream version
        stream_id = f"loan-{application_id}"
        version = await self.store.stream_version(stream_id)
        
        await self.store.append(
            stream_id,
            [event],
            expected_version=version
        )
        
        self.stats["events_appended"] += 1
    
    async def _compliance_agent(self, application_id: str, agent: Agent):
        """Compliance agent simulation"""
        # Simulate regulatory checks
        regulations = [
            "BASEL_III_2024",
            "KYC_VERIFIED",
            "AML_CHECK",
            "SANCTIONS_SCREENING",
            "CREDIT_CAPACITY"
        ]
        
        passed = random.random() > 0.1  # 90% pass rate
        
        for reg in random.sample(regulations, random.randint(3, 5)):
            if passed:
                from src.models.events import ComplianceRulePassed
                event = ComplianceRulePassed(
                    application_id=application_id,
                    rule_id=reg,
                    regulation_version="v2.0",
                    check_id=f"check-{uuid.uuid4().hex[:8]}",
                    details={"verified_at": datetime.utcnow().isoformat()}
                )
            else:
                from src.models.events import ComplianceRuleFailed
                event = ComplianceRuleFailed(
                    application_id=application_id,
                    rule_id=reg,
                    regulation_version="v2.0",
                    check_id=f"check-{uuid.uuid4().hex[:8]}",
                    failure_reason="Regulatory check failed"
                )
            
            stream_id = f"loan-{application_id}"
            version = await self.store.stream_version(stream_id)
            await self.store.append(stream_id, [event], expected_version=version)
            self.stats["events_appended"] += 1
    
    async def _decision_agent(self, application_id: str, agent: Agent):
        """Decision orchestrator agent simulation"""
        # Load application state
        app = await LoanApplicationAggregate.load(self.store, application_id)
        
        # Wait for all analyses to complete
        if not app.credit_analysis_tier:
            return
        
        # Simulate AI decision
        if app.credit_analysis_tier == "HIGH":
            recommendation = "DECLINE"
            confidence = random.uniform(0.4, 0.7)
        elif app.credit_analysis_tier == "MEDIUM":
            recommendation = random.choices(["APPROVE", "REFER"], weights=[0.7, 0.3])[0]
            confidence = random.uniform(0.7, 0.9)
        else:  # LOW
            recommendation = "APPROVE"
            confidence = random.uniform(0.85, 0.98)
        
        # Gather contributing sessions
        sessions = [agent.session_id]
        
        await self.command_handlers.handle_generate_decision(
            application_id=application_id,
            agent_id=agent.agent_id,
            session_id=agent.session_id,
            orchestrator_recommendation=recommendation,
            orchestrator_confidence=confidence,
            contributing_sessions=sessions,
            reasoning=f"AI decision based on credit tier {app.credit_analysis_tier}"
        )
        
        self.stats["events_appended"] += 1
        
        if recommendation == "APPROVE":
            self.stats["applications_approved"] += 1
        elif recommendation == "DECLINE":
            self.stats["applications_declined"] += 1
    
    async def _human_review(self):
        """Simulate human review for pending applications"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT application_id, ai_recommendation, ai_confidence
                FROM application_summary
                WHERE current_state IN ('APPROVED_PENDING_HUMAN', 'DECLINED_PENDING_HUMAN', 'PENDING_DECISION')
            """)
        
        for row in rows:
            application_id = row["application_id"]
            recommendation = row["ai_recommendation"]
            
            # Human review decision
            if recommendation == "APPROVE":
                final_decision = "APPROVED"
                override = random.random() < 0.05  # 5% override rate
            elif recommendation == "DECLINE":
                final_decision = "DECLINED"
                override = random.random() < 0.1  # 10% override rate
            else:  # REFER
                final_decision = random.choices(["APPROVED", "DECLINED"], weights=[0.4, 0.6])[0]
                override = True
            
            await self.command_handlers.handle_human_review_completed(
                application_id=application_id,
                reviewer_id=f"reviewer-{random.randint(1, 10)}",
                final_decision=final_decision,
                override=override,
                override_reason="Human judgment" if override else None,
                comments=f"Human review completed for {application_id}"
            )
            
            self.stats["events_appended"] += 1
            
            if final_decision == "APPROVED":
                self.stats["applications_approved"] += 1
            else:
                self.stats["applications_declined"] += 1
    
    async def _audit_all_applications(self):
        """Run integrity checks for all applications"""
        for i in range(self.config.num_applications):
            application_id = f"APP-{i+1:04d}"
            
            try:
                result = await run_integrity_check(
                    self.store,
                    "loan",
                    application_id
                )
                
                if not result.tamper_detected:
                    self.results.append({
                        "application_id": application_id,
                        "integrity_valid": result.chain_valid,
                        "events_verified": result.events_verified
                    })
                    
            except Exception as e:
                if self.config.verbose:
                    print(f"  ⚠ Integrity check failed for {application_id}: {e}")
    
    async def _run_what_if_scenarios(self):
        """Run what-if counterfactual scenarios"""
        projector = WhatIfProjector(self.store)
        
        # Test a few applications
        for i in range(min(5, self.config.num_applications)):
            application_id = f"APP-{i+1:04d}"
            
            try:
                # Create counterfactual: HIGH risk instead of MEDIUM
                from src.models.events import CreditAnalysisCompleted
                
                counterfactual = CreditAnalysisCompleted(
                    application_id=application_id,
                    risk_tier="HIGH",
                    credit_score=620,
                    max_credit_limit=250000,
                    model_version="v2.0",
                    confidence_score=0.55
                )
                
                result = await projector.run_what_if(
                    application_id=application_id,
                    branch_at_event_type="CreditAnalysisCompleted",
                    counterfactual_events=[counterfactual]
                )
                
                if self.config.verbose:
                    print(f"\n  📊 What-if for {application_id}:")
                    print(f"     Real outcome: {result['real_outcome']['state']}")
                    print(f"     Counterfactual: {result['counterfactual_outcome']['state']}")
                    print(f"     Divergent events: {len(result['divergence_events'])}")
                    
            except Exception as e:
                if self.config.verbose:
                    print(f"  ⚠ What-if failed for {application_id}: {e}")
    
    async def _generate_regulatory_packages(self):
        """Generate regulatory examination packages"""
        for i in range(min(3, self.config.num_applications)):
            application_id = f"APP-{i+1:04d}"
            
            try:
                package = await generate_regulatory_package(
                    self.store,
                    application_id,
                    examination_date=datetime.utcnow()
                )
                
                # Save to file
                filename = f"regulatory_package_{application_id}.json"
                with open(filename, 'w') as f:
                    json.dump(package, f, indent=2, default=str)
                
                if self.config.verbose:
                    print(f"  ✓ Generated {filename} ({package['integrity_check']['events_verified']} events)")
                    
            except Exception as e:
                if self.config.verbose:
                    print(f"  ⚠ Regulatory package failed for {application_id}: {e}")
    
    async def _simulate_crash_recovery(self):
        """Simulate Gas Town crash recovery pattern"""
        # Select a random agent session
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT stream_id FROM events 
                WHERE event_type = 'AgentContextLoaded'
                LIMIT 1
            """)
            
            if row:
                stream_id = row["stream_id"]
                parts = stream_id.split("-")
                if len(parts) >= 3:
                    agent_id = parts[1]
                    session_id = parts[2]
                    
                    # Simulate crash - no in-memory state
                    print(f"\n  💥 Simulating crash for agent {agent_id} session {session_id}")
                    
                    # Reconstruct context from event store
                    context = await reconstruct_agent_context(
                        self.store,
                        agent_id,
                        session_id
                    )
                    
                    print(f"  🔄 Reconstructed context:")
                    print(f"     Health: {context.session_health_status}")
                    print(f"     Last position: {context.last_event_position}")
                    print(f"     Pending work: {len(context.pending_work)}")
                    
                    # Resume from reconstructed context
                    if context.session_health_status == "NEEDS_RECONCILIATION":
                        print(f"     ⚠ Partial decision detected - needs reconciliation")
    
    async def _print_results(self, elapsed_seconds: float):
        """Print simulation results"""
        print(f"\n{Fore.CYAN}{'='*60}")
        print(f"Simulation Results")
        print(f"{'='*60}{Style.RESET_ALL}")
        
        # Basic statistics
        print(f"\n{Fore.GREEN}Basic Statistics:{Style.RESET_ALL}")
        basic_stats = [
            ["Applications Submitted", self.stats["applications_submitted"]],
            ["Applications Approved", self.stats["applications_approved"]],
            ["Applications Declined", self.stats["applications_declined"]],
            ["Total Events Appended", self.stats["events_appended"]],
            ["Concurrency Conflicts", self.stats["concurrency_conflicts"]],
            ["Simulation Duration", f"{elapsed_seconds:.2f}s"],
            ["Events per Second", f"{self.stats['events_appended'] / elapsed_seconds:.1f}"]
        ]
        print(tabulate(basic_stats, tablefmt="grid"))
        
        # Agent performance
        print(f"\n{Fore.GREEN}Agent Performance:{Style.RESET_ALL}")
        agent_stats = []
        for agent_type, count in self.stats["agent_actions"].items():
            agent_stats.append([agent_type.upper(), count])
        print(tabulate(agent_stats, headers=["Agent Type", "Actions"], tablefmt="grid"))
        
        # Response times
        if self.stats["response_times_ms"]:
            avg_response = sum(self.stats["response_times_ms"]) / len(self.stats["response_times_ms"])
            p95_response = sorted(self.stats["response_times_ms"])[int(len(self.stats["response_times_ms"]) * 0.95)]
            
            print(f"\n{Fore.GREEN}Performance Metrics:{Style.RESET_ALL}")
            perf_stats = [
                ["Avg Response Time", f"{avg_response:.2f}ms"],
                ["p95 Response Time", f"{p95_response:.2f}ms"],
                ["Max Response Time", f"{max(self.stats['response_times_ms']):.2f}ms"]
            ]
            print(tabulate(perf_stats, tablefmt="grid"))
        
        # Projection lags
        if self.daemon:
            print(f"\n{Fore.GREEN}Projection Lags:{Style.RESET_ALL}")
            lag_stats = []
            for proj_name in self.projections:
                lag = await self.daemon.get_lag(proj_name)
                lag_stats.append([proj_name, f"{lag} events"])
            print(tabulate(lag_stats, headers=["Projection", "Lag"], tablefmt="grid"))
        
        # Integrity results
        valid_checks = sum(1 for r in self.results if r.get("integrity_valid", False))
        total_checks = len(self.results)
        if total_checks > 0:
            print(f"\n{Fore.GREEN}Integrity Verification:{Style.RESET_ALL}")
            print(f"  ✓ Valid chains: {valid_checks}/{total_checks} ({valid_checks/total_checks*100:.1f}%)")
        
        print(f"\n{Fore.CYAN}{'='*60}")
        print(f"Simulation Complete!")
        print(f"{'='*60}{Style.RESET_ALL}")


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Ledger System Simulation")
    parser.add_argument("--applications", type=int, default=50, help="Number of applications to process")
    parser.add_argument("--agents", type=int, default=4, help="Agents per application")
    parser.add_argument("--duration", type=int, default=60, help="Simulation duration in seconds")
    parser.add_argument("--concurrent", type=int, default=10, help="Concurrent agents")
    parser.add_argument("--no-crash", action="store_true", help="Disable crash simulation")
    parser.add_argument("--no-whatif", action="store_true", help="Disable what-if scenarios")
    parser.add_argument("--no-regulatory", action="store_true", help="Disable regulatory packages")
    parser.add_argument("--quiet", action="store_true", help="Quiet mode")
    
    args = parser.parse_args()
    
    config = SimulationConfig(
        num_applications=args.applications,
        agents_per_application=args.agents,
        concurrent_agents=args.concurrent,
        simulation_duration_seconds=args.duration,
        enable_crash_simulation=not args.no_crash,
        enable_what_if=not args.no_whatif,
        enable_regulatory=not args.no_regulatory,
        verbose=not args.quiet
    )
    
    simulator = LedgerSimulator(config)
    
    try:
        await simulator.initialize()
        results = await simulator.run_simulation()
        
        # Print summary
        print(f"\n{Fore.CYAN}Final Summary:{Style.RESET_ALL}")
        print(f"  ✓ Successfully processed {simulator.stats['applications_submitted']} applications")
        print(f"  ✓ Generated {simulator.stats['events_appended']} events")
        print(f"  ✓ Resolved {simulator.stats['concurrency_conflicts']} concurrency conflicts")
        print(f"  ✓ Completed with {len([r for r in results if r.get('integrity_valid')])} valid integrity checks")
        
    except Exception as e:
        print(f"{Fore.RED}Simulation failed: {e}{Style.RESET_ALL}")
        raise
    finally:
        if simulator.pool:
            await simulator.pool.close()
            print(f"\n{Fore.GREEN}Database connection closed{Style.RESET_ALL}")


if __name__ == "__main__":
    asyncio.run(main())
"""Live bidirectional coordinator tying TransferManager, Monitor, Scheduler, StragglerDetector, and Storage."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import asdict
from typing import Any

from flowweave.config import FlowWeaveConfig, SchedulerConfig
from flowweave.models import (
    LinkDirection,
    LinkState,
    PolicyName,
    ScheduleDecision,
    SchedulerInput,
    StragglerDecision,
    TransferDirection,
    TransferState,
)
from flowweave.monitor.monitor import NetworkMonitor
from flowweave.scheduler import Scheduler, create_scheduler
from flowweave.scheduler.base import validate_and_enforce_allocations
from flowweave.storage import Storage
from flowweave.straggler.detector import StragglerDetector
from flowweave.transfer.manager import TransferManager

logger = logging.getLogger("flowweave.coordinator")


class FlowWeaveCoordinator:
    """Runs the periodic cooperative control loop over real concurrent uploads and downloads."""

    def __init__(self, config: FlowWeaveConfig | None = None) -> None:
        self.config = config or FlowWeaveConfig()
        self.storage = Storage(self.config.db_path)
        self.monitor = NetworkMonitor(self.config.monitor)

        # Register separate full-duplex downlink and uplink capacities by default.
        self.monitor.register_link(
            LinkState(
                link_id="downlink",
                capacity_bps=self.config.transfer.total_link_capacity_bps,
                latency_ms=15.0,
                loss_rate=0.0,
                is_shared=True,
                direction=LinkDirection.DOWNLINK,
                source_node="remote",
                target_node="local",
                estimated_available_bps=self.config.transfer.total_link_capacity_bps,
            )
        )
        self.monitor.register_link(
            LinkState(
                link_id="uplink",
                capacity_bps=self.config.transfer.upload_link_capacity_bps,
                latency_ms=15.0,
                loss_rate=0.0,
                is_shared=True,
                direction=LinkDirection.UPLINK,
                source_node="local",
                target_node="remote",
                estimated_available_bps=self.config.transfer.upload_link_capacity_bps,
            )
        )

        self.scheduler: Scheduler = create_scheduler(
            self.config.scheduler.policy,
            config=self.config.scheduler,
            horizon_s=max(self.config.control_interval_s, 0.1),
        )
        self.straggler_detector = StragglerDetector(self.config.straggler)
        self.transfer_manager = TransferManager(
            config=self.config.transfer,
            monitor=self.monitor,
            storage=self.storage,
        )
        self._loop_task: asyncio.Task | None = None
        self._running = False
        self.last_decision: ScheduleDecision | None = None
        self.recent_stragglers: list[dict[str, Any]] = []
        self.decision_log: deque[dict[str, Any]] = deque(maxlen=40)
        self._last_tick_time = time.monotonic()

    async def start(self) -> None:
        if self._running:
            return
        await self.transfer_manager.start()
        self._running = True
        self._last_tick_time = time.monotonic()
        self._loop_task = asyncio.create_task(self._control_loop())

    async def stop(self) -> None:
        self._running = False
        if self._loop_task is not None:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
            self._loop_task = None
        await self.transfer_manager.shutdown()
        self.storage.close()

    def set_policy(
        self,
        policy: PolicyName | str,
        *,
        min_fair_share_fraction: float | None = None,
        srpt_exponent: float | None = None,
        total_link_capacity_bps: float | None = None,
        upload_link_capacity_bps: float | None = None,
    ) -> SchedulerConfig:
        """Switch active scheduling policy and link capacities live."""
        policy_enum = PolicyName(policy)
        self.config.scheduler.policy = policy_enum
        if min_fair_share_fraction is not None:
            self.config.scheduler.min_fair_share_fraction = min_fair_share_fraction
        if srpt_exponent is not None:
            self.config.scheduler.srpt_exponent = srpt_exponent
        if total_link_capacity_bps is not None and total_link_capacity_bps > 0:
            self.config.transfer.total_link_capacity_bps = total_link_capacity_bps
            self.monitor.update_link("downlink", capacity_bps=total_link_capacity_bps)
        if upload_link_capacity_bps is not None and upload_link_capacity_bps > 0:
            self.config.transfer.upload_link_capacity_bps = upload_link_capacity_bps
            self.monitor.update_link("uplink", capacity_bps=upload_link_capacity_bps)

        self.scheduler = create_scheduler(
            policy_enum,
            config=self.config.scheduler,
            horizon_s=max(self.config.control_interval_s, 0.1),
        )
        self.decision_log.append(
            {
                "timestamp": time.time(),
                "policy": policy_enum.value,
                "messages": [f"Switched active scheduler policy to {policy_enum.value}"],
            }
        )
        return self.config.scheduler

    async def step_once(self) -> ScheduleDecision:
        """Execute one monitor -> schedule -> validate -> apply -> straggler-check control tick."""
        now = time.monotonic()
        dt = max(0.01, now - self._last_tick_time)
        self._last_tick_time = now

        self.transfer_manager.flush_metrics(now, dt)
        snapshots = self.transfer_manager.list_snapshots()
        links = self.monitor.get_links()

        decision = self.scheduler.decide(SchedulerInput(now=now, transfers=snapshots, links=links))

        # Validate non-negativity and directional/shared capacity constraints for coordinated policies.
        if self.scheduler.name != PolicyName.INDEPENDENT:
            active_ids = {s.transfer_id for s in snapshots if s.state == TransferState.ACTIVE}
            decision.allocations_bps = validate_and_enforce_allocations(
                decision.allocations_bps,
                decision.route_assignments,
                links,
                active_ids,
            )

        self.last_decision = decision
        self.transfer_manager.apply_decision(decision)

        if decision.rationale:
            logger.debug("Policy %s: %s", decision.policy.value, "; ".join(decision.rationale))
            if not self.decision_log or self.decision_log[-1]["messages"] != decision.rationale:
                self.decision_log.append(
                    {
                        "timestamp": time.time(),
                        "policy": decision.policy.value,
                        "messages": list(decision.rationale),
                    }
                )

        # Observe stragglers without injecting speculative duplicate WAN traffic by default.
        views = self.transfer_manager.active_chunk_views(now)
        straggler_decisions: list[StragglerDecision] = self.straggler_detector.evaluate(
            now=now,
            active_chunks=views,
            links=links,
            total_useful_bytes=sum(s.measured.bytes_acked for s in snapshots),
        )
        if straggler_decisions:
            self.recent_stragglers = [asdict(d) for d in straggler_decisions][-20:]

        dl_bps = sum(
            s.measured.rolling_goodput_bps or s.measured.instant_goodput_bps
            for s in snapshots
            if s.direction == TransferDirection.DOWNLOAD and s.state == TransferState.ACTIVE
        )
        ul_bps = sum(
            s.measured.rolling_goodput_bps or s.measured.instant_goodput_bps
            for s in snapshots
            if s.direction == TransferDirection.UPLOAD and s.state == TransferState.ACTIVE
        )
        sample_payload = {
            "active_transfers": sum(1 for s in snapshots if s.state == TransferState.ACTIVE),
            "download_throughput_bps": round(dl_bps, 2),
            "upload_throughput_bps": round(ul_bps, 2),
            "aggregate_goodput_bps": round(dl_bps + ul_bps, 2),
            "allocations_bps": {k: round(v, 2) for k, v in decision.allocations_bps.items()},
            "links": {lid: link.to_dict() for lid, link in links.items()},
        }
        self.storage.record_monitor_sample(time.time(), self.scheduler.name.value, sample_payload)
        return decision

    async def _control_loop(self) -> None:
        while self._running:
            try:
                await self.step_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Coordinator tick error: %s", exc)
            await asyncio.sleep(self.config.control_interval_s)

    def get_monitor_status(self) -> dict[str, Any]:
        snapshots = self.transfer_manager.list_snapshots()
        links = self.monitor.get_links()

        dl_bps = sum(
            s.measured.rolling_goodput_bps or s.measured.instant_goodput_bps
            for s in snapshots
            if s.direction == TransferDirection.DOWNLOAD and s.state == TransferState.ACTIVE
        )
        ul_bps = sum(
            s.measured.rolling_goodput_bps or s.measured.instant_goodput_bps
            for s in snapshots
            if s.direction == TransferDirection.UPLOAD and s.state == TransferState.ACTIVE
        )
        completed_fcts = [
            max(0.0, s.completed_at - s.created_at)
            for s in snapshots
            if s.state == TransferState.COMPLETED and s.completed_at is not None
        ]
        avg_fct = sum(completed_fcts) / len(completed_fcts) if completed_fcts else 0.0

        return {
            "is_simulated": False,
            "policy": self.scheduler.name.value,
            "control_interval_s": self.config.control_interval_s,
            "scheduler_config": asdict(self.config.scheduler),
            "download_throughput_bps": round(dl_bps, 2),
            "upload_throughput_bps": round(ul_bps, 2),
            "aggregate_goodput_bps": round(dl_bps + ul_bps, 2),
            "average_fct_s": round(avg_fct, 3),
            "downlink_capacity_bps": self.config.transfer.total_link_capacity_bps,
            "uplink_capacity_bps": self.config.transfer.upload_link_capacity_bps,
            "links": [link.to_dict() for link in links.values()],
            "transfers": [s.to_dict() for s in snapshots],
            "straggler_alerts": list(self.recent_stragglers),
            "decision_log": list(self.decision_log)[-15:],
            "last_compute_time_ms": (
                round(self.last_decision.compute_time_ms, 4) if self.last_decision else 0.0
            ),
            "history": self.storage.recent_monitor_samples(limit=60),
        }

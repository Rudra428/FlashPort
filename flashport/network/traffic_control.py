"""Generate opt-in Linux tc/netem commands; this module never executes privileged actions."""

from __future__ import annotations

from dataclasses import dataclass
import math
import re
import sys

_DEVICE_RE = re.compile(r"[A-Za-z0-9_.:-]{1,15}\Z")


@dataclass(frozen=True)
class TcProfile:
    bandwidth_mbit: float
    delay_ms: float = 0.0
    packet_loss_percent: float = 0.0

    def __post_init__(self) -> None:
        if not math.isfinite(self.bandwidth_mbit) or self.bandwidth_mbit <= 0:
            raise ValueError("bandwidth_mbit must be finite and positive")
        if not math.isfinite(self.delay_ms) or self.delay_ms < 0:
            raise ValueError("delay_ms must be finite and non-negative")
        if (
            not math.isfinite(self.packet_loss_percent)
            or not 0 <= self.packet_loss_percent <= 100
        ):
            raise ValueError("packet_loss_percent must be between 0 and 100")


def build_tc_commands(profile: TcProfile, device: str) -> tuple[list[str], list[str]]:
    """Return apply/remove argv lists for a user to review and run explicitly on Linux."""
    if sys.platform != "linux":
        raise RuntimeError("tc/netem profiles are supported only on Linux.")
    if not _DEVICE_RE.fullmatch(device):
        raise ValueError("device must be a simple Linux interface name")
    apply = [
        "sudo", "tc", "qdisc", "replace", "dev", device, "root", "netem",
        "rate", f"{profile.bandwidth_mbit:g}mbit",
    ]
    if profile.delay_ms:
        apply.extend(["delay", f"{profile.delay_ms:g}ms"])
    if profile.packet_loss_percent:
        apply.extend(["loss", f"{profile.packet_loss_percent:g}%"])
    remove = ["sudo", "tc", "qdisc", "del", "dev", device, "root"]
    return apply, remove


def describe_tc_setup() -> str:
    return (
        "Optional Linux-only experiment: review a tc/netem command for a dedicated test "
        "interface, then run it manually with administrator approval. FlashPort never "
        "invokes sudo or changes host network settings."
    )

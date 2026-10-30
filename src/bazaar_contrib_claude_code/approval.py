from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from time import time_ns

from bazaar_contrib_harness import ApprovalRequest, ApprovalResult
from pydantic import JsonValue

from .protocol import ClaudeProtocolError, JsonObject


@dataclass(frozen=True, slots=True)
class ApprovalEnvelope:
    """Claude permission request plus its provider-neutral projection."""

    control_request_id: str
    tool_name: str
    tool_input: Mapping[str, object]
    request: ApprovalRequest


def parse_approval_request(
    message: Mapping[str, object],
    *,
    session_id: str,
    turn_id: str,
) -> ApprovalEnvelope:
    if message.get("type") != "control_request":
        raise ClaudeProtocolError("message is not a control request")
    control_request_id = message.get("request_id")
    if not isinstance(control_request_id, str) or not control_request_id:
        raise ClaudeProtocolError("permission control request requires a request_id")
    request = message.get("request")
    if not isinstance(request, Mapping) or request.get("subtype") != "can_use_tool":
        raise ClaudeProtocolError("unsupported Claude control request subtype")
    tool_use_id = request.get("tool_use_id")
    if not isinstance(tool_use_id, str) or not tool_use_id:
        raise ClaudeProtocolError("permission request requires a tool_use_id")
    tool_name = request.get("tool_name")
    if not isinstance(tool_name, str) or not tool_name:
        raise ClaudeProtocolError("permission request requires a tool_name")
    tool_input = request.get("input")
    if not isinstance(tool_input, Mapping):
        raise ClaudeProtocolError("permission request input must be an object")

    details = _details(request, tool_input)
    return ApprovalEnvelope(
        control_request_id=control_request_id,
        tool_name=tool_name,
        tool_input=tool_input,
        request=ApprovalRequest(
            request_id=tool_use_id,
            session_id=session_id,
            action=tool_name,
            created_at_ms=time_ns() // 1_000_000,
            turn_id=turn_id,
            details=details,
        ),
    )


def build_approval_response(
    approval: ApprovalEnvelope, result: ApprovalResult
) -> JsonObject:
    if result.request_id != approval.request.request_id:
        raise ValueError("approval result request id does not match tool use")
    if result.decision == "approved":
        return {"behavior": "allow", "updatedInput": dict(approval.tool_input)}
    return {
        "behavior": "deny",
        "message": result.reason or "Permission denied by the user.",
    }


_MAX_APPROVAL_DETAIL = 4_000


def _details(
    request: Mapping[str, object], tool_input: Mapping[str, object]
) -> dict[str, JsonValue]:
    reasons: list[str] = []
    for field_name in ("description", "title", "display_name", "decision_reason"):
        value = request.get(field_name)
        if isinstance(value, str) and value.strip() and value.strip() not in reasons:
            reasons.append(value.strip())
    try:
        encoded_input = json.dumps(
            dict(tool_input),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except TypeError, ValueError:
        encoded_input = ""
    details: dict[str, JsonValue] = {}
    if reasons:
        details["reason"] = "\n".join(reasons)
    tool_name = request["tool_name"]
    if isinstance(tool_name, str) and tool_name:
        details["tool_name"] = tool_name
    if encoded_input:
        details["tool_input"] = (
            encoded_input[: _MAX_APPROVAL_DETAIL - 1] + "…"
            if len(encoded_input) > _MAX_APPROVAL_DETAIL
            else encoded_input
        )
    return details


__all__ = [
    "ApprovalEnvelope",
    "build_approval_response",
    "parse_approval_request",
]

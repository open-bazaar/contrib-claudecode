"""Project native initialize commands as the workspace's skills."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, cast

from bazaar_contrib_harness import HarnessSkill

# where a command came from, as `initialize` marks it at the end of its
# description; the CLI's own commands are marked `builtin` and are not skills
_COMMAND_SOURCE = re.compile(r"\s*\((project|user)\)\s*$")


def _read_skills(answer: Mapping[str, object]) -> tuple[HarnessSkill, ...]:
    """The skills among the commands Claude Code answers to: all but its
    own. The shape is what `initialize` returns under `response.commands`
    (claude 2.1.280): `name`, `description`, `argumentHint`, and `builtin`
    for the CLI's own; a description ending in `(project)` is the
    workspace's, in `(user)` the person's, anything else came with it."""

    response = cast(Mapping[str, Any], answer["response"])
    skills: list[HarnessSkill] = []
    for command in response["commands"]:
        if command.get("builtin"):
            continue
        description = command["description"]
        marked = _COMMAND_SOURCE.search(description)
        skills.append(
            HarnessSkill(
                name=command["name"],
                description=description[: marked.start()] if marked else description,
                source=(
                    "system"
                    if marked is None
                    else "workspace"
                    if marked[1] == "project"
                    else "personal"
                ),
            )
        )
    return tuple(skills)

import logging
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import anyio
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from plugin import HotTopicsPlugin, PLUGIN_VERSION


@pytest.mark.parametrize("global_mode", [True, False])
@pytest.mark.parametrize("excluded", ["10001", "blocked-session"])
@pytest.mark.parametrize("private", [True, False])
def test_excluded_chat_never_receives_context(global_mode: bool, excluded: str, private: bool) -> None:
    # Given: a blocked chat and an allowed neighbour discovered through the SDK.
    plugin = HotTopicsPlugin()
    plugin.set_plugin_config({"plugin": {"config_version": PLUGIN_VERSION}, "ambient": {
        "apply_to_all": global_mode,
        "private_ids" if private else "group_ids": ["10001", "10002"],
        "excluded_chat_ids": [f" {excluded} "],
        "proactive_chance": 0,
    }})
    streams = [
        {"stream_id": "blocked-session", "user_id" if private else "group_id": "10001"},
        {"stream_id": "allowed-session", "user_id" if private else "group_id": "10002"},
    ]
    append = AsyncMock(return_value={"success": True})
    plugin._ctx = SimpleNamespace(
        logger=logging.getLogger("test"),
        chat=SimpleNamespace(
            get_all_streams=AsyncMock(return_value=streams),
            get_stream_by_group_id=AsyncMock(side_effect=streams),
            get_stream_by_user_id=AsyncMock(side_effect=streams),
        ),
        maisaka=SimpleNamespace(context=SimpleNamespace(append=append)),
    )
    # When: an ambient item is delivered through the real plugin flow.
    anyio.run(plugin._deliver_ambient, [{"title": "Test item"}], "test")
    # Then: the blocked chat never reaches the host context capability.
    assert [call.args[0] for call in append.call_args_list] == ["allowed-session"]


@pytest.mark.parametrize("excluded", ["10001", "blocked-session"])
def test_excluded_chat_never_receives_proactive_trigger(excluded: str) -> None:
    # Given: proactive delivery targets an explicitly excluded chat.
    plugin = HotTopicsPlugin()
    plugin.set_plugin_config({"plugin": {"config_version": PLUGIN_VERSION}, "ambient": {
        "excluded_chat_ids": [excluded],
        "proactive_groups": ["10001"],
        "proactive_chance": 100,
    }})
    trigger = AsyncMock()
    plugin._ctx = SimpleNamespace(
        logger=logging.getLogger("test"),
        chat=SimpleNamespace(get_stream_by_group_id=AsyncMock(return_value={
            "stream_id": "blocked-session", "group_id": "10001",
        })),
        maisaka=SimpleNamespace(proactive=SimpleNamespace(trigger=trigger)),
    )
    # When: the real delivery flow attempts proactive sharing.
    anyio.run(plugin._deliver_ambient, [{"title": "Test item"}], "test")
    # Then: no proactive request reaches the host.
    trigger.assert_not_awaited()


def test_empty_exclusions_preserve_existing_delivery() -> None:
    # Given: an old configuration without the new optional field.
    plugin = HotTopicsPlugin()
    plugin.set_plugin_config({"plugin": {"config_version": PLUGIN_VERSION}, "ambient": {
        "apply_to_all": True,
        "proactive_chance": 0,
    }})
    append = AsyncMock(return_value={"success": True})
    plugin._ctx = SimpleNamespace(
        logger=logging.getLogger("test"),
        chat=SimpleNamespace(get_all_streams=AsyncMock(return_value=[
            {"stream_id": "existing-session", "group_id": "10001"},
        ])),
        maisaka=SimpleNamespace(context=SimpleNamespace(append=append)),
    )
    # When: ambient delivery runs with the default empty exclusion list.
    anyio.run(plugin._deliver_ambient, [{"title": "Test item"}], "test")
    # Then: existing chats continue receiving context.
    assert append.await_args.args[0] == "existing-session"


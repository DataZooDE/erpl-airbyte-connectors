"""A failed partition must not let a server-side position be checkpointed.

The CDK calls `ensure_at_least_one_state_emitted()` in `_on_stream_is_done`
regardless of whether the stream raised -- it only downgrades the stream status
to INCOMPLETE. For an ODP stream that is data loss: the delta pointer would
advance past packets that were fetched but never emitted, and the next sync
would never see them again.
"""

from unittest.mock import MagicMock

import pytest
from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.message import InMemoryMessageRepository
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

from source_sap.cursors import DriverStateCursor
from source_sap.protocols.base import ReadPlan, SapObject
from source_sap.streams import ErplPartition


def _cursor():
    repo, mgr = InMemoryMessageRepository(), ConnectorStateManager()
    driver = MagicMock()
    driver.next_state.return_value = {"subscriber_process": "AB_X"}
    obj = SapObject(name="S", json_schema={}, meta={})
    return DriverStateCursor("S", None, repo, mgr, driver, MagicMock(), obj, {}), repo, driver


def _exploding_session(message="boom"):
    session = MagicMock()
    session.cursor.return_value.execute.side_effect = RuntimeError(message)
    return session


def test_a_read_failure_marks_the_cursor_failed():
    cursor, repo, driver = _cursor()
    partition = ErplPartition("S", _exploding_session(), ReadPlan(sql="SELECT 1"), None, cursor=cursor)
    with pytest.raises(AirbyteTracedException):
        list(partition.read())
    cursor.ensure_at_least_one_state_emitted()
    assert list(repo.consume_queue()) == []
    driver.on_success.assert_not_called()


def test_a_successful_read_still_checkpoints():
    cursor, repo, driver = _cursor()
    session = MagicMock()
    result = session.cursor.return_value.execute.return_value
    result.description = [("A", "VARCHAR")]
    result.fetchmany.side_effect = [[("x",)], []]
    partition = ErplPartition("S", session, ReadPlan(sql="SELECT 1"), None, cursor=cursor)
    assert len(list(partition.read())) == 1
    cursor.ensure_at_least_one_state_emitted()
    assert len(list(repo.consume_queue())) == 1
    driver.on_success.assert_called_once()


def test_one_failed_partition_out_of_several_suppresses_the_checkpoint():
    cursor, repo, _ = _cursor()
    good = MagicMock()
    good_result = good.cursor.return_value.execute.return_value
    good_result.description = [("A", "VARCHAR")]
    good_result.fetchmany.side_effect = [[("x",)], []]

    list(ErplPartition("S", good, ReadPlan(sql="a"), None, cursor=cursor).read())
    with pytest.raises(AirbyteTracedException):
        list(ErplPartition("S", _exploding_session(), ReadPlan(sql="b"), None, cursor=cursor).read())

    cursor.ensure_at_least_one_state_emitted()
    assert list(repo.consume_queue()) == []


def test_a_cursor_without_mark_failed_is_tolerated():
    # FieldValueCursor and NoStateCursor have no mark_failed; the partition must
    # not break on them.
    session = _exploding_session()
    partition = ErplPartition("S", session, ReadPlan(sql="SELECT 1"), None, cursor=MagicMock(spec=[]))
    with pytest.raises(AirbyteTracedException):
        list(partition.read())


def test_partitions_built_by_the_generator_carry_the_cursor():
    """Regression: the generator used to drop the cursor, so nothing could mark
    the run failed and a broken delta sync still checkpointed."""
    from source_sap.streams import build_stream

    cursor, repo, _ = _cursor()
    driver = MagicMock()
    driver.prepare = None
    driver.read_plans.return_value = [ReadPlan(sql="SELECT 1")]
    driver.concurrency_group.return_value = ""
    obj = SapObject(name="S", json_schema={}, meta={})

    stream = build_stream(_exploding_session(), driver, obj, cursor, incremental=True, state={})
    partitions = list(stream.generate_partitions())
    assert partitions

    with pytest.raises(AirbyteTracedException):
        list(partitions[0].read())
    cursor.ensure_at_least_one_state_emitted()
    assert list(repo.consume_queue()) == [], "a failed generated partition must suppress state"

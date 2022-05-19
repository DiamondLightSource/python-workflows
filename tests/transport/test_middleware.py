from __future__ import annotations

import functools
import logging
import time
from unittest import mock

import pytest

from workflows.transport import middleware
from workflows.transport.offline_transport import OfflineTransport


def test_counter_middleware():
    offline = OfflineTransport()
    offline.connect()
    counter = middleware.CounterMiddleware()
    offline.add_middleware(counter)

    for i in range(10):
        offline.send(str(mock.sentinel.channel), str(mock.sentinel.message))
        assert counter.send_count == i + 1

    offline.ack(str(mock.sentinel.messagid), 1)
    assert counter.ack_count == 1

    offline.nack(str(mock.sentinel.messagid), 1)
    assert counter.nack_count == 1

    offline.broadcast(str(mock.sentinel.channel), str(mock.sentinel.message))
    assert counter.broadcast_count == 1

    mock_callback = mock.Mock()
    offline.subscribe(str(mock.sentinel.channel), mock_callback)
    assert counter.subscribe_count == 1

    txid = offline.transaction_begin()
    offline.transaction_abort(txid)
    assert counter.transaction_begin_count == 1
    assert counter.transaction_abort_count == 1

    txid = offline.transaction_begin()
    offline.transaction_commit(txid)
    assert counter.transaction_begin_count == 2
    assert counter.transaction_commit_count == 1


def test_timer_middleware(caplog):
    offline = OfflineTransport()
    offline.connect()
    timer = middleware.TimerMiddleware()
    offline.add_middleware(timer)

    def callback(header, message):
        time.sleep(1)

    subscription_id = offline.subscribe(str(mock.sentinel.channel), callback)
    with caplog.at_level(logging.INFO):
        offline.subscription_callback(subscription_id)(
            {"destination": "foo"}, str(mock.sentinel.message)
        )
        assert "Callback for foo took:" in caplog.text


def test_prometheus_middleware():
    prometheus_client = pytest.importorskip("prometheus_client")

    from workflows.transport.middleware import prometheus

    offline = OfflineTransport()
    offline.connect()
    instrument = prometheus.PrometheusMiddleware(source="foo")
    offline.add_middleware(instrument)

    for i in range(10):
        offline.send(str(mock.sentinel.channel), str(mock.sentinel.message))

    offline.ack(str(mock.sentinel.messagid), 1)
    offline.nack(str(mock.sentinel.messagid), 1)
    offline.broadcast(str(mock.sentinel.channel), str(mock.sentinel.message))

    def callback(header, message):
        print(header)
        time.sleep(1)

    sid_1 = offline.subscribe(str(mock.sentinel.channel), callback)
    offline.subscription_callback(sid_1)(
        {"destination": "foo"}, str(mock.sentinel.message)
    )

    sid_2 = offline.subscribe_broadcast(str(mock.sentinel.channel), callback)
    offline.subscription_callback(sid_2)(
        {"destination": "bar"}, str(mock.sentinel.message)
    )

    ts = offline.subscribe_temporary(str(mock.sentinel.channel), callback)
    offline.subscription_callback(ts.subscription_id)(
        {"destination": "foobar"}, str(mock.sentinel.message)
    )

    offline.unsubscribe(sid_2)

    txid = offline.transaction_begin()
    offline.transaction_abort(txid)

    txid = offline.transaction_begin()
    offline.transaction_commit(txid)

    txid = offline.transaction_begin()

    data = prometheus_client.generate_latest().decode("ascii")
    expected_output = """
workflows_callback_processing_time_seconds_bucket{le="+Inf",source="test_middleware:test_prometheus_middleware.<locals>.callback"} 3.0
workflows_callback_processing_time_seconds_count{source="test_middleware:test_prometheus_middleware.<locals>.callback"} 3.0
workflows_callback_processing_time_seconds_sum{source="test_middleware:test_prometheus_middleware.<locals>.callback"}
workflows_transport_active_subscriptions{source="foo"} 2.0
workflows_transport_subscriptions_total{source="foo"} 1.0
workflows_transport_temporary_subscriptions_total{source="foo"} 1.0
workflows_transport_broadcast_subscriptions_total{source="foo"} 1.0
workflows_transport_ack_total{source="foo"} 1.0
workflows_transport_nack_total{source="foo"} 1.0
workflows_transport_send_total{source="foo"} 10.0
workflows_transport_transaction_begin_total{source="foo"} 3.0
workflows_transport_transaction_abort_total{source="foo"} 1.0
workflows_transport_transaction_commit_total{source="foo"} 1.0
workflows_transport_transactions_in_progress{source="foo"} 1.0
"""
    for line in expected_output.splitlines():
        assert line in data


def example_callback(header, message):
    pass


example_functools_partial_callback = functools.partial(
    example_callback, message={"foo": "bar"}
)
example_nested_functools_partial_callback = functools.partial(
    example_functools_partial_callback, header={"ham": "spam"}
)


def test_get_callback_source():
    from workflows.transport.middleware import prometheus

    assert (
        prometheus.PrometheusMiddleware.get_callback_source(example_callback)
        == "test_middleware:example_callback"
    )
    assert (
        prometheus.PrometheusMiddleware.get_callback_source(
            example_functools_partial_callback
        )
        == "test_middleware:example_callback"
    )
    assert (
        prometheus.PrometheusMiddleware.get_callback_source(
            example_nested_functools_partial_callback
        )
        == "test_middleware:example_callback"
    )

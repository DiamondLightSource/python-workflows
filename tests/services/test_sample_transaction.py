from __future__ import annotations

from unittest import mock

import workflows.services
import workflows.services.sample_transaction


def test_services_can_be_looked_up():
    """Attempt to look up the services by their names"""
    service_class = workflows.services.lookup("SampleTxn")
    assert service_class == workflows.services.sample_transaction.SampleTxn
    service_class = workflows.services.lookup("SampleTxnProducer")
    assert service_class == workflows.services.sample_transaction.SampleTxnProducer


def test_txnproducer_registers_idle_timer():
    """Check that the TXN producer registers an idle event handler."""
    p = workflows.services.sample_transaction.SampleTxnProducer()
    mock_idlereg = mock.Mock()
    setattr(p, "_register_idle", mock_idlereg)

    p.initializing()

    mock_idlereg.assert_called_once_with(mock.ANY, p.create_message)


def test_txnproducer_produces_messages():
    """Check that the TXN producer produces messages in the idle event handler."""
    p = workflows.services.sample_transaction.SampleTxnProducer()
    mock_transport = mock.Mock()
    setattr(p, "_transport", mock_transport)

    p.initializing()
    assert not mock_transport.send.called
    p.create_message()

    mock_transport.send.assert_called_once()

    p.create_message()

    assert mock_transport.send.call_count == 2
    calls = mock_transport.send.call_args_list
    assert calls[0][0][0] == calls[1][0][0]  # same destination
    assert calls[0][0][1] != calls[1][0][1]  # different message


def test_txnservice_subscribes_to_channel():
    """Check that the service subscribes to a queue with acknowledgements enabled."""
    p = workflows.services.sample_transaction.SampleTxn()
    mock_transport = mock.Mock()
    setattr(p, "_transport", mock_transport)

    p.initializing()

    mock_transport.subscribe.assert_called_once_with(
        mock.ANY, p.receive_message, acknowledgement=True, prefetch_count=1000
    )


def test_txnservice_crash_function_crashes_sometimes():
    """The crash should happen sometimes. Neither never nor always."""
    fn = workflows.services.sample_transaction.SampleTxn.crashpoint

    assert any(fn() for i in range(100))
    assert not all(fn() for i in range(100))


def setup_txnservice(crashpattern):
    """Common fixture for TXN tests"""
    p = workflows.services.sample_transaction.SampleTxn()
    mock_crash, mock_transport = mock.Mock(), mock.Mock()
    p.crashpoint = mock_crash
    mock_crash.side_effect = crashpattern
    mock_transport.transaction_begin.return_value = mock.sentinel.txn
    mock_transport.subscribe.return_value = mock.sentinel.subid
    setattr(p, "_transport", mock_transport)
    header = {"message-id": mock.sentinel.message_id}
    message = mock.sentinel.message

    p.initializing()
    p.receive_message(header, message)

    return p, mock_transport


def test_txnservice_uses_transactions_correctly():
    """The TXN service should consume messages in a transaction. When the service fails the messages must not be consumed."""
    p, mock_transport = setup_txnservice([True])

    mock_transport.transaction_begin.assert_called_once_with()
    mock_transport.ack.assert_not_called()
    mock_transport.send.assert_not_called()
    mock_transport.transaction_commit.assert_not_called()
    mock_transport.transaction_abort.assert_called_once_with(mock.sentinel.txn)

    p, mock_transport = setup_txnservice([False, True])

    mock_transport.transaction_begin.assert_called_once_with()
    mock_transport.ack.assert_called_once_with(
        mock.sentinel.message_id, mock.sentinel.subid, transaction=mock.sentinel.txn
    )
    mock_transport.send.assert_not_called()
    mock_transport.transaction_commit.assert_not_called()
    mock_transport.transaction_abort.assert_called_once_with(mock.sentinel.txn)

    p, mock_transport = setup_txnservice([False, False, True])

    mock_transport.transaction_begin.assert_called_once_with()
    mock_transport.ack.assert_called_once_with(
        mock.sentinel.message_id, mock.sentinel.subid, transaction=mock.sentinel.txn
    )
    mock_transport.send.assert_called_once_with(
        mock.ANY, mock.sentinel.message, transaction=mock.sentinel.txn
    )
    mock_transport.transaction_commit.assert_not_called()
    mock_transport.transaction_abort.assert_called_once_with(mock.sentinel.txn)

    p, mock_transport = setup_txnservice([False, False, False])

    mock_transport.transaction_begin.assert_called_once_with()
    mock_transport.ack.assert_called_once_with(
        mock.sentinel.message_id, mock.sentinel.subid, transaction=mock.sentinel.txn
    )
    mock_transport.send.assert_called_once_with(
        mock.ANY, mock.sentinel.message, transaction=mock.sentinel.txn
    )
    mock_transport.transaction_commit.assert_called_once_with(mock.sentinel.txn)
    mock_transport.transaction_abort.assert_not_called()

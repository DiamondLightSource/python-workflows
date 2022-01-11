from __future__ import annotations

import random
import time

from workflows.services.common_service import CommonService


class SampleTxn(CommonService):
    """An example service building on top of the workflow.services architecture,
    demonstrating how this architecture can be used.
    This service consumes a messages off one queue and places it into another.
    Transactions are used to guarantee correct message handling.
    The service is deliberately unreliable and prone to failure."""

    # Human readable service name
    _service_name = "Transaction sample"

    def initializing(self):
        """Subscribe to a channel. Received messages must be acknowledged."""
        self.subid = self._transport.subscribe(
            "transient.transaction",
            self.receive_message,
            acknowledgement=True,
            prefetch_count=1000,
        )

    @staticmethod
    def crashpoint():
        """Return true if the service should malfunction at this point."""
        # Probability of not crashing is 90%
        return random.uniform(0, 1) > 0.90

    def receive_message(self, header, message):
        """Receive a message"""

        self.log.info("=== Receive ===")
        self.log.info(header)
        self.log.info(message)

        self.log.info("MsgID: {}".format(header["message-id"]))
        assert header["message-id"]

        txn = self._transport.transaction_begin()
        self.log.info(f" 1. Txn: {txn}")
        if self.crashpoint():
            self._transport.transaction_abort(txn)
            self.log.info("---  Abort  ---")
            return

        self._transport.ack(header["message-id"], self.subid, transaction=txn)
        self.log.info(" 2. Ack")
        if self.crashpoint():
            self._transport.transaction_abort(txn)
            self.log.info("---  Abort  ---")
            return

        self._transport.send("transient.destination", message, transaction=txn)
        self.log.info(" 3. Send")

        if self.crashpoint():
            self._transport.transaction_abort(txn)
            self.log.info("---  Abort  ---")
            return

        self._transport.transaction_commit(txn)
        self.log.info(" 4. Commit")
        self.log.info("===  Done   ===")


class SampleTxnProducer(CommonService):
    """An example service building on top of the workflow.services architecture,
    demonstrating how this architecture can be used.
    This service generates messages for the Transaction example."""

    # Human readable service name
    _service_name = "TXN Message Producer"

    counter = 0

    def initializing(self):
        """Service initialization. This function is run before any commands are
        received from the frontend. This is the place to request channel
        subscriptions with the messaging layer, and register callbacks.
        This function can be overridden by specific service implementations."""
        self._register_idle(3, self.create_message)

    def create_message(self):
        """Create and send a unique message for this service."""
        self.counter += 1
        self._transport.send(
            "transient.transaction",
            "TXMessage #%d\n++++++++Produced@ %f"
            % (self.counter, (time.time() % 1000) * 1000),
        )
        self.log.info("Created message %d", self.counter)

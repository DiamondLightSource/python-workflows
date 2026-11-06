"""
Provides a RabbitMQ broker for the transport integration tests.

The broker is located, in order of preference:

1. RABBITMQ_HOST / RABBITMQ_PORT environment variables, if either is set
2. An existing broker answering on localhost:5672
3. A temporary rabbitmq container, started with docker or podman

If none of these are available, or the container fails to start (e.g.
the image can't be pulled while offline), then the tests needing a broker
are skipped - unless the CI environment variable is set, in which case they
fail, so that a broken CI broker cannot silently skip the tests.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import shutil
import subprocess
import time
from collections.abc import Iterator
from typing import NoReturn

import pika
import pika.exceptions
import pytest

logger = logging.getLogger(__name__)

RABBITMQ_IMAGE = "rabbitmq:3"
BROKER_STARTUP_TIMEOUT = 60


@dataclasses.dataclass(frozen=True)
class RabbitMQBroker:
    host: str
    port: int
    username: str = "guest"
    password: str = "guest"

    def connection_parameters(self, **kwargs) -> pika.ConnectionParameters:
        return pika.ConnectionParameters(
            self.host,
            self.port,
            "/",
            pika.PlainCredentials(self.username, self.password),
            **kwargs,
        )

    def is_available(self) -> bool:
        """Check whether an AMQP connection can be made to this broker."""
        try:
            pika.BlockingConnection(
                self.connection_parameters(
                    connection_attempts=1, socket_timeout=1, stack_timeout=2
                )
            ).close()
        except pika.exceptions.AMQPError:
            return False
        return True

    def wait_until_available(self, timeout: float) -> bool:
        """Retry connecting until the broker accepts connections, or timeout."""
        start = time.monotonic()
        while not self.is_available():
            if time.monotonic() - start > timeout:
                return False
            time.sleep(0.5)
        logger.info(
            "RabbitMQ at %s:%d available after %.1fs",
            self.host,
            self.port,
            time.monotonic() - start,
        )
        return True


def _no_broker(reason: str) -> NoReturn:
    if os.environ.get("CI"):
        pytest.fail(f"No RabbitMQ broker available in CI: {reason}")
    pytest.skip(f"No RabbitMQ broker available: {reason}")


def _find_container_runtime() -> str | None:
    """Find a container runtime command with a running daemon."""
    for runtime in ["docker", "podman"]:
        if command := shutil.which(runtime):
            if (
                subprocess.run([command, "version"], capture_output=True).returncode
                == 0
            ):
                return command
    return None


class ContainerStartupError(RuntimeError):
    """The broker container could not be started, or never became available."""


def _container_host_port(runtime: str, container_id: str) -> int:
    """Read the host port that the container's AMQP port is published on."""
    result = subprocess.run(
        [runtime, "port", container_id, "5672/tcp"],
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise ContainerStartupError(
            f"Could not read container port mapping: {result.stderr.strip()}"
        )
    # e.g. "127.0.0.1:49153", possibly followed by further (IPv6) bindings
    return int(result.stdout.splitlines()[0].rsplit(":", 1)[1])


def _start_broker_container(runtime: str) -> tuple[RabbitMQBroker, str]:
    """
    Start a broker container and wait for it to accept connections.

    Returns:
        The broker, and the container ID to remove when finished with it.

    Raises:
        ContainerStartupError: If the broker could not be made available. Any
            container that was started is removed before raising.
    """
    logger.info("Starting %s container with %s", RABBITMQ_IMAGE, runtime)
    result = subprocess.run(
        [runtime, "run", "-d", "--rm", "-p", "127.0.0.1::5672", RABBITMQ_IMAGE],
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise ContainerStartupError(
            f"'{runtime} run {RABBITMQ_IMAGE}' failed (exit {result.returncode}): "
            f"{result.stderr.strip()}"
        )
    container_id = result.stdout.strip()
    try:
        broker = RabbitMQBroker(
            "127.0.0.1", _container_host_port(runtime, container_id)
        )
        if not broker.wait_until_available(BROKER_STARTUP_TIMEOUT):
            raise ContainerStartupError(
                f"{RABBITMQ_IMAGE} container did not accept connections "
                f"within {BROKER_STARTUP_TIMEOUT}s"
            )
    except BaseException:
        _remove_container(runtime, container_id)
        raise
    return broker, container_id


def _remove_container(runtime: str, container_id: str) -> None:
    subprocess.run([runtime, "rm", "-f", container_id], capture_output=True)


@pytest.fixture(scope="session")
def rabbitmq_broker() -> Iterator[RabbitMQBroker]:
    """A RabbitMQ broker, for tests that need a real server."""
    if "RABBITMQ_HOST" in os.environ or "RABBITMQ_PORT" in os.environ:
        broker = RabbitMQBroker(
            os.environ.get("RABBITMQ_HOST", "localhost"),
            int(os.environ.get("RABBITMQ_PORT", 5672)),
        )
        # An explicitly configured broker may still be starting (e.g. a CI
        # service container whose health check passed early), so retry
        if not broker.wait_until_available(BROKER_STARTUP_TIMEOUT):
            _no_broker(f"Could not connect to {broker.host}:{broker.port}")
        yield broker
        return

    broker = RabbitMQBroker("localhost", 5672)
    if broker.is_available():
        yield broker
        return

    runtime = _find_container_runtime()
    if runtime is None:
        _no_broker(
            "Nothing on localhost:5672, RABBITMQ_HOST/RABBITMQ_PORT unset, "
            "and no running docker or podman to start one"
        )
    try:
        broker, container_id = _start_broker_container(runtime)
    except ContainerStartupError as e:
        _no_broker(str(e))
    else:
        try:
            yield broker
        finally:
            _remove_container(runtime, container_id)

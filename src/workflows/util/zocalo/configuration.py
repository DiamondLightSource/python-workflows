"""
Zocalo configuration for workflows objects

Only imported if Zocalo is present in the environment.
"""

from __future__ import annotations

from typing import Any

from marshmallow import fields  # type: ignore
from zocalo.configuration import PluginSchema  # type: ignore

import workflows.transport
from workflows.transport.pika_transport import PikaTransport
from workflows.transport.stomp_transport import StompTransport


class OTEL:
    """A Zocalo configuration plugin to pre-populate OTELTracing config defaults"""

    class Schema(PluginSchema):
        host = fields.Str(required=True)
        port = fields.Int(required=True)
        timeout = fields.Int(required=False, load_default=10)

    # Store configuration for access by services
    config: dict[str, Any] = {}

    @staticmethod
    def activate(configuration):
        # Build the full endpoint URL
        endpoint = f"https://{configuration['host']}:{configuration['port']}/v1/traces"
        OTEL.config["endpoint"] = endpoint
        OTEL.config["timeout"] = configuration.get("timeout", 10)
        return OTEL.config


class Stomp:
    """A Zocalo configuration plugin to pre-populate StompTransport config defaults"""

    class Schema(PluginSchema):
        host = fields.Str(required=True)
        port = fields.Int(required=True)
        username = fields.Str(required=True)
        password = fields.Str(required=True)
        prefix = fields.Str(required=True)

    @staticmethod
    def activate(configuration):
        for cfgoption, target in [
            ("host", "--stomp-host"),
            ("port", "--stomp-port"),
            ("username", "--stomp-user"),
            ("password", "--stomp-pass"),
            ("prefix", "--stomp-prfx"),
        ]:
            StompTransport.defaults[target] = configuration[cfgoption]


class Pika:
    """A Zocalo configuration plugin to pre-populate PikaTransport config defaults"""

    class Schema(PluginSchema):
        host = fields.Str(required=True)
        port = fields.Field(required=True)
        username = fields.Str(required=True)
        password = fields.Str(required=True)
        vhost = fields.Str(required=True)

    @staticmethod
    def activate(configuration):
        for cfgoption, target in [
            ("host", "--rabbit-host"),
            ("port", "--rabbit-port"),
            ("username", "--rabbit-user"),
            ("password", "--rabbit-pass"),
            ("vhost", "--rabbit-vhost"),
        ]:
            PikaTransport.defaults[target] = configuration[cfgoption]


class DefaultTransport:
    """A Zocalo configuration plugin to set a default transport class"""

    class Schema(PluginSchema):
        default = fields.Str(required=True)

    @staticmethod
    def activate(configuration):
        workflows.transport.default_transport = configuration["default"]

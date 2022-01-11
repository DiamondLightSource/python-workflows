from __future__ import annotations

from marshmallow import fields
from zocalo.configuration import PluginSchema

import workflows.transport
from workflows.transport.pika_transport import PikaTransport
from workflows.transport.stomp_transport import StompTransport


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

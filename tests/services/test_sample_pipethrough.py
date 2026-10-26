from __future__ import annotations

from unittest import mock

from workflows.services.sample_pipethrough import SamplePipethrough


def test_service_subscribes_to_recipe_channel(mocker):
    wrap_subscribe = mocker.patch("workflows.recipe.wrap_subscribe")
    service = SamplePipethrough()
    service.transport = mock.Mock()
    service.initializing()
    wrap_subscribe.assert_called_once_with(
        service.transport,
        "transient.destination",
        service.process,
        acknowledgement=False,
        log_extender=service.extend_log,
    )

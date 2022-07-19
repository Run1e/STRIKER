import asyncio
from dataclasses import dataclass
from unittest.mock import AsyncMock, Mock

import pytest
from adapters.broker import Broker

from tests.testutils import *

UPDATE_INTERVAL = 0.1


@dataclass(frozen=True)
class Success:
    id: int


@dataclass(frozen=True)
class Failure:
    id: int


@dataclass(frozen=True)
class Enqueued:
    id: int
    infront: int


@dataclass(frozen=True)
class Processing:
    id: int


@pytest.fixture
def broker():
    b = Broker(
        queue_prefix='prefix',
        send_queue='send',
        recv_queue='recv',
        success_event=Success,
        failure_event=Failure,
        enqueue_event=Enqueued,
        processing_event=Processing,
        id_type_cast=int,
        update_interval=UPDATE_INTERVAL,
        max_updates=2,
    )

    return b


def event_at(mock, pos):
    return mock.call_args_list[pos][0][0]


@pytest.mark.asyncio
async def test_broker_processing(broker: Broker, mock_dispatch):
    broker._handle_send_event(1, dispatcher=mock_dispatch)

    mock_dispatch.assert_called_once()
    processing_event = mock_dispatch.call_args[0][0]
    assert isinstance(processing_event, Processing)
    assert processing_event.id == 1


@pytest.mark.asyncio
async def test_broker_enqueued(broker: Broker, mock_dispatch):
    broker._handle_send_event(1, dispatcher=mock_dispatch)
    broker._handle_send_event(2, dispatcher=mock_dispatch)

    processing_event = event_at(mock_dispatch, 0)
    assert isinstance(processing_event, Processing)
    assert processing_event.id == 1

    enqueued_event = event_at(mock_dispatch, 1)
    assert isinstance(enqueued_event, Enqueued)
    assert enqueued_event.id == 2
    assert enqueued_event.infront == 1


@pytest.mark.asyncio
async def test_broker_on_recv(broker: Broker, mock_dispatch):
    broker._handle_send_event(1, dispatcher=mock_dispatch)
    broker._handle_send_event(2, dispatcher=mock_dispatch)
    broker._handle_recv_event(1)

    processing_event = event_at(mock_dispatch, 0)
    assert isinstance(processing_event, Processing)
    assert processing_event.id == 1

    enqueued_event = event_at(mock_dispatch, 1)
    assert isinstance(enqueued_event, Enqueued)
    assert enqueued_event.id == 2
    assert enqueued_event.infront == 1

    processing_event = event_at(mock_dispatch, 2)
    assert isinstance(processing_event, Processing)
    assert processing_event.id == 2


@pytest.mark.asyncio
async def test_broker_on_recv_multiple(broker: Broker, mock_dispatch):
    broker._handle_send_event(1, dispatcher=mock_dispatch)
    broker._handle_send_event(2, dispatcher=mock_dispatch)
    broker._handle_send_event(3, dispatcher=mock_dispatch)
    broker._handle_recv_event(1)

    processing_event = event_at(mock_dispatch, 0)
    assert isinstance(processing_event, Processing)
    assert processing_event.id == 1

    enqueued_event = event_at(mock_dispatch, 1)
    assert isinstance(enqueued_event, Enqueued)
    assert enqueued_event.id == 2
    assert enqueued_event.infront == 1

    enqueued_event = event_at(mock_dispatch, 2)
    assert isinstance(enqueued_event, Enqueued)
    assert enqueued_event.id == 3

    processing_event = event_at(mock_dispatch, 3)
    assert isinstance(processing_event, Processing)
    assert processing_event.id == 2
    # nothing else should happen here since we didn't cross the update timeout thing


@pytest.mark.asyncio
async def test_broker_recv_then_send(broker: Broker, mock_dispatch):
    broker._handle_send_event(1, dispatcher=mock_dispatch)
    broker._handle_send_event(2, dispatcher=mock_dispatch)
    broker._handle_send_event(3, dispatcher=mock_dispatch)

    await asyncio.sleep(UPDATE_INTERVAL * 2)

    broker._handle_recv_event(1)
    broker._handle_send_event(4, dispatcher=mock_dispatch)

    processing_event = event_at(mock_dispatch, 0)
    assert isinstance(processing_event, Processing)
    assert processing_event.id == 1

    enqueued_event = event_at(mock_dispatch, 1)
    assert isinstance(enqueued_event, Enqueued)
    assert enqueued_event.id == 2
    assert enqueued_event.infront == 1

    enqueued_event = event_at(mock_dispatch, 2)
    assert isinstance(enqueued_event, Enqueued)
    assert enqueued_event.id == 3

    processing_event = event_at(mock_dispatch, 3)
    assert isinstance(processing_event, Processing)
    assert processing_event.id == 2

    enqueued_event = event_at(mock_dispatch, 4)
    assert isinstance(enqueued_event, Enqueued)
    assert enqueued_event.id == 3
    assert enqueued_event.infront == 1

    enqueued_event = event_at(mock_dispatch, 5)
    assert isinstance(enqueued_event, Enqueued)
    assert enqueued_event.id == 4
    assert enqueued_event.infront == 2


@pytest.mark.asyncio
async def test_broker_on_recv_multiple_with_update_delay(broker: Broker, mock_dispatch):
    broker._handle_send_event(1, dispatcher=mock_dispatch)
    broker._handle_send_event(2, dispatcher=mock_dispatch)
    broker._handle_send_event(3, dispatcher=mock_dispatch)

    await asyncio.sleep(UPDATE_INTERVAL * 2)

    broker._handle_recv_event(1)

    processing_event = event_at(mock_dispatch, 0)
    assert isinstance(processing_event, Processing)
    assert processing_event.id == 1

    enqueued_event = event_at(mock_dispatch, 1)
    assert isinstance(enqueued_event, Enqueued)
    assert enqueued_event.id == 2
    assert enqueued_event.infront == 1

    enqueued_event = event_at(mock_dispatch, 2)
    assert isinstance(enqueued_event, Enqueued)
    assert enqueued_event.id == 3

    processing_event = event_at(mock_dispatch, 3)
    assert isinstance(processing_event, Processing)
    assert processing_event.id == 2


@pytest.mark.asyncio
async def test_broker_on_recv_multiple_out_of_order(broker: Broker, mock_dispatch):
    broker._handle_send_event(1, dispatcher=mock_dispatch)
    broker._handle_send_event(2, dispatcher=mock_dispatch)
    broker._handle_send_event(3, dispatcher=mock_dispatch)

    await asyncio.sleep(UPDATE_INTERVAL * 2)

    broker._handle_recv_event(2)

    processing_event = event_at(mock_dispatch, 0)
    assert isinstance(processing_event, Processing)
    assert processing_event.id == 1

    enqueued_event = event_at(mock_dispatch, 1)
    assert isinstance(enqueued_event, Enqueued)
    assert enqueued_event.id == 2
    assert enqueued_event.infront == 1

    enqueued_event = event_at(mock_dispatch, 2)
    assert isinstance(enqueued_event, Enqueued)
    assert enqueued_event.id == 3
    assert enqueued_event.infront == 2

    enqueued_event = event_at(mock_dispatch, 3)
    assert isinstance(enqueued_event, Processing)
    assert enqueued_event.id == 3


@pytest.mark.asyncio
async def test_broker_enqueued_updates_before_interval(broker: Broker, mock_dispatch):
    broker._handle_send_event(1, dispatcher=mock_dispatch)
    broker._handle_send_event(2, dispatcher=mock_dispatch)
    broker._handle_send_event(3, dispatcher=mock_dispatch)
    broker._handle_send_event(4, dispatcher=mock_dispatch)
    broker._handle_send_event(5, dispatcher=mock_dispatch)
    broker._handle_recv_event(1)

    a = event_at(mock_dispatch, 0)
    assert isinstance(a, Processing)
    assert a.id == 1

    a = event_at(mock_dispatch, 1)
    assert isinstance(a, Enqueued)
    assert a.id == 2
    assert a.infront == 1

    a = event_at(mock_dispatch, 2)
    assert isinstance(a, Enqueued)
    assert a.id == 3
    assert a.infront == 2

    a = event_at(mock_dispatch, 3)
    assert isinstance(a, Enqueued)
    assert a.id == 4
    assert a.infront == 3

    a = event_at(mock_dispatch, 4)
    assert isinstance(a, Enqueued)
    assert a.id == 5
    assert a.infront == 4

    a = event_at(mock_dispatch, 5)
    assert isinstance(a, Processing)
    assert a.id == 2

    await asyncio.sleep(UPDATE_INTERVAL * 2)
    broker._handle_recv_event(2)

    a = event_at(mock_dispatch, 6)
    assert isinstance(a, Processing)
    assert a.id == 3

    a = event_at(mock_dispatch, 7)
    assert isinstance(a, Enqueued)
    assert a.id == 4
    assert a.infront == 1

    a = event_at(mock_dispatch, 8)
    assert isinstance(a, Enqueued)
    assert a.id == 5
    assert a.infront == 2

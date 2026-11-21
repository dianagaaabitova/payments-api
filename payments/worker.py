import asyncio
import logging
import signal

from sqlalchemy.ext.asyncio import async_sessionmaker

from payments.delivery.messaging import create_broker, declare_dead_letter_topology, relay
from payments.delivery.webhook import HttpWebhookSender
from payments.persistence.db import create_engine
from payments.processing import PaymentProcessor
from payments.settings import get_settings


async def run() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.set)

    processor = PaymentProcessor(
        sessions,
        HttpWebhookSender(
            settings.webhook_signing_secret.get_secret_value(),
            settings.webhook_timeout,
            settings.webhook_allowed_hosts,
        ),
        settings.retry_base_delay,
    )
    broker = create_broker(settings.rabbitmq_url, processor)
    try:
        async with broker:
            await declare_dead_letter_topology(broker)
            await broker.start()
            task = asyncio.create_task(relay(sessions, broker))
            try:
                await shutdown.wait()
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(run())

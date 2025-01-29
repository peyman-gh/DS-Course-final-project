import json
import asyncio
import os
from aiogram import Bot, Dispatcher, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiokafka import AIOKafkaConsumer

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME", "signals")

# Bot initialization
TOKEN = "8071488723:AAEaaLfiiOCRPDQRneL59ncbv_BWVzn5Ac4"
dp = Dispatcher()

# Store for user chat IDs
user_chat_ids = set()

# Variable to store the last signal text
last_sig_text = None

async def kafka_consumer(bot):
    global last_sig_text
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVER,
        group_id="my-group",
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            try:
                signal_data = msg.value
                stock_symbol = signal_data.get('stock_symbol')  # stock_symbol
                signal = signal_data.get('signal')

                if stock_symbol and signal in ['buy', 'sell']:
                    sig_text = f"{signal}: {stock_symbol}"

                    # Check if the sig_text is the same as the last one
                    if sig_text != last_sig_text:
                        for chat_id in user_chat_ids:
                            await bot.send_message(chat_id=chat_id, text=sig_text)
                        print(f"Signal sent to all bot users: {sig_text}")
                        # Update the last_sig_text
                        last_sig_text = sig_text

                else:
                    print(f"Invalid signal data: {signal_data}")

            except Exception as e:
                print(f"Error processing Kafka message: {e}")

    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    user_chat_ids.add(message.chat.id)
    await message.answer(f"Hello, {html.bold(message.from_user.full_name)}!\n You've subscribed to stock market signals notification bot.")

async def main() -> None:
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    telegram_task = asyncio.create_task(dp.start_polling(bot))
    kafka_task = asyncio.create_task(kafka_consumer(bot))
    await asyncio.gather(kafka_task, telegram_task)

if __name__ == "__main__":
    asyncio.run(main())

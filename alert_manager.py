from telegram import Bot
import asyncio

class AlertManager:
    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = chat_id
        self.bot = Bot(token=self.token)

    async def send_async_message(self, text):
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=text)
        except Exception as e:
            print(f"[!] Error enviando mensaje a Telegram: {e}")

    def send_message(self, text):
        asyncio.run(self.send_async_message(text))

    # añade este método a tu clase AlertManager:
    def sync_send(self, text: str):
        # wrapper síncrono simple
        try:
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self.send_async_message(text))
            else:
                loop.run_until_complete(self.send_async_message(text))
        except Exception:
            pass

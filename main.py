"""Telegram account sender. Importing this module never connects to Telegram."""
import argparse
import asyncio
import contextlib
import io
import json
import logging
import os
from pathlib import Path
import random
import tempfile
import time

ROOT = Path(__file__).resolve().parent
MATERIALS = Path(os.environ.get("MATERIALS_DIR", str(ROOT / "Материалы"))).resolve()
DATA = Path(os.environ.get("DATA_DIR", str(ROOT / "data"))).resolve()
CYCLE_SLEEP = 23 * 24 * 60 * 60
FORMATS = {".jpg", ".jpeg", ".png", ".webp"}
LOG = logging.getLogger(__name__)


def load_materials(folder=MATERIALS):
    chats = list(dict.fromkeys(
        line.strip() for line in (folder / "chats.txt").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ))
    message = (folder / "message.txt").read_text(encoding="utf-8").strip()
    paths = sorted(p for p in (folder / "photos").iterdir()
                   if p.is_file() and p.suffix.lower() in FORMATS)
    if not chats or not message or len(paths) < 3:
        raise ValueError("Нужны непустые chats.txt, message.txt и минимум 3 фотографии")
    photos = []
    for path in paths[:3]:
        photo = io.BytesIO(path.read_bytes())
        photo.name = path.name
        if not photo.getbuffer().nbytes:
            raise ValueError(f"Пустая фотография: {path.name}")
        photos.append(photo)
    return chats, message, photos


def new_state():
    return {"version": 1, "sent": [], "next_at": 0, "in_flight": None}


def load_state(path):
    if not path.exists():
        return new_state()
    state = json.loads(path.read_text(encoding="utf-8"))
    if (state.get("version") != 1 or not isinstance(state.get("sent"), list)
            or not all(isinstance(chat, str) for chat in state["sent"])
            or not isinstance(state.get("next_at"), (int, float))
            or not (state.get("in_flight") is None or isinstance(state["in_flight"], str))):
        raise ValueError("Некорректный state.json; не удаляйте его без проверки прогресса")
    return state


def save_state(path, state):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=".state-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            json.dump(state, file, ensure_ascii=False, indent=2)
            file.flush()
            os.fsync(file.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


@contextlib.contextmanager
def exclusive_lock(folder):
    import fcntl
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (folder / "process.lock").open("a") as file:
        try:
            fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("Другой процесс уже использует этот DATA_DIR") from error
        yield


def make_client():
    from telethon import TelegramClient
    try:
        api_id = int(os.environ["TELEGRAM_API_ID"])
        api_hash = os.environ["TELEGRAM_API_HASH"]
        if api_id <= 0 or not api_hash.strip():
            raise ValueError()
    except (KeyError, ValueError) as error:
        raise ValueError("Задайте TELEGRAM_API_ID и TELEGRAM_API_HASH") from error
    return TelegramClient(str(DATA / "session"), api_id, api_hash)


async def notify(client, text):
    LOG.info(text)
    admin = os.environ.get("TELEGRAM_ADMIN_CHAT")
    if admin:
        try:
            await client.send_message(admin, text)
        except Exception as error:
            LOG.warning("Не удалось отправить уведомление: %s", type(error).__name__)


async def run_sender(client, path):
    from telethon.errors import FloodWaitError
    state = load_state(path)
    if state["in_flight"]:
        raise RuntimeError("Неопределённый результат прошлой отправки. Проверьте чат и выполните resolve.")
    await notify(client, "Сервис рассылки запущен")
    while True:
        remaining = state["next_at"] - time.time()
        if remaining > 0:
            await asyncio.sleep(min(remaining, 30))
            continue
        try:
            chats, message, photos = load_materials()
        except (OSError, ValueError) as error:
            LOG.error("Материалы недоступны: %s. Повтор через 60 секунд", error)
            await asyncio.sleep(60)
            continue
        chat = next((chat for chat in chats if chat not in state["sent"]), None)
        if chat is None:
            state = new_state()
            state["next_at"] = time.time() + CYCLE_SLEEP
            save_state(path, state)
            await notify(client, "Цикл завершён. Следующий цикл через 23 дня")
            continue
        state["in_flight"] = chat
        save_state(path, state)
        try:
            await client.send_file(chat, photos, caption=message)
        except FloodWaitError as error:
            state["in_flight"] = None
            state["next_at"] = time.time() + max(error.seconds, 60)
            save_state(path, state)
            await notify(client, f"Telegram ограничил отправку; ожидание {error.seconds} секунд")
            continue
        except Exception as error:
            # Network failures may happen after delivery. Leave the durable marker intact.
            raise RuntimeError("Результат отправки неизвестен. Проверьте чат и выполните resolve.") from error
        state["sent"].append(chat)
        state["in_flight"] = None
        delay = random.randint(3600, 10800)
        state["next_at"] = time.time() + delay
        # Start the long pause immediately if this was the last chat in the snapshot.
        if all(item in state["sent"] for item in chats):
            state = new_state()
            state["next_at"] = time.time() + CYCLE_SLEEP
            text = "Последний чат обработан. Следующий цикл через 23 дня"
        else:
            text = f"Отправлено. Следующая отправка не раньше чем через {delay // 60} минут"
        save_state(path, state)
        await notify(client, text)


async def telegram_command(command):
    client = make_client()
    try:
        if command == "login":
            await client.start()
            LOG.info("Авторизация сохранена. Рассылка НЕ запускалась")
        else:
            await client.connect()
            if not await client.is_user_authorized():
                raise RuntimeError("Нет действующей сессии. Сначала выполните: python main.py login")
            await run_sender(client, DATA / "state.json")
    finally:
        await client.disconnect()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["login", "run", "check", "status", "resolve"])
    parser.add_argument("--outcome", choices=["sent", "retry"])
    args = parser.parse_args()
    if args.command == "resolve" and not args.outcome:
        parser.error("resolve требует --outcome sent или --outcome retry")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    os.umask(0o077)
    try:
        if args.command == "check":
            chats, _, photos = load_materials()
            LOG.info("Материалы прочитаны: %d чатов, %d фото. Telegram не подключался", len(chats), len(photos))
            return
        with exclusive_lock(DATA):
            if args.command in {"status", "resolve"}:
                path = DATA / "state.json"
                state = load_state(path)
                if args.command == "resolve":
                    chat = state["in_flight"]
                    if not chat:
                        raise ValueError("Нет незавершённой отправки")
                    if args.outcome == "sent" and chat not in state["sent"]:
                        state["sent"].append(chat)
                    state["in_flight"] = None
                    state["next_at"] = time.time() + random.randint(3600, 10800)
                    save_state(path, state)
                print(json.dumps(state, ensure_ascii=False, indent=2))
            else:
                asyncio.run(telegram_command(args.command))
    except KeyboardInterrupt:
        LOG.info("Остановлено")
    except Exception as error:
        LOG.error("%s", error)
        raise SystemExit(1)


if __name__ == "__main__":
    main()

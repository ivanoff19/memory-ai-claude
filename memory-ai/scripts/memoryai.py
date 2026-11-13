#!/usr/bin/env python3
"""Плагин memory-ai для Claude Code: сессии с этого Мака — в Memory AI → «Чаты».

Хуки (hooks/hooks.json) шлют события сессии на сервер (assistant-server/ext_claude.py):
  SessionStart → start, UserPromptSubmit → prompt, Stop → stop (итоговый ответ Claude),
  Notification → notify, SessionEnd → end.
Ответ с телефона доходит до сессии двумя путями:
  - Stop-хук получает его в ответе сервера и не даёт сессии остановиться (decision: block);
  - после Stop фоновый ожидатель (asyncRewake) ждёт ответа и будит сессию (exit 2, текст в
    stderr). Ожидателей может быть много (по одному на простаивающую сессию), сервер
    спрашивает только один из них — тот, кто держит pump.lock, за всех.
PermissionRequest: вопрос уходит на телефон, хук ждёт «да / нет». В терминале — если человек не у
Мака (клавиатура и мышь не трогались 2+ минуты), вернулся — обычный диалог. В Claude Desktop и
VS Code диалог на Маке открыт рядом с хуком (кто ответил первым, тот и решил) — телефон
спрашиваем после 30 с без касаний Мака (с 0.3.7). Нет сети — пробуем снова, а не теряем вопрос.
Что было с каждым вопросом — в ~/.memory-ai/log.txt («perm …»).

Ход сессии (с 0.2.0) берётся из её transcript (transcript_path во входе каждого хука), а не из
текста хуков: настоящие сообщения человека с картинками, весь текст Claude и его пояснения по
ходу (с 0.3.9), шаги (инструменты), снимки из ответов инструментов (с 0.3.9), файлы SendUserFile,
план, модель. Читается с запомненного смещения (~/.memory-ai/sync), шлётся
в /cc/sync. Первое событие сессии подтягивает хвост (последние 50 сообщений).
«Стоп» с телефона: сервер отвечает stop → флаг в ~/.memory-ai/stop, PreToolUse не пускает
следующий инструмент, Claude коротко подводит итог и останавливается.

Подключение без команд (с 0.3.0): синхронный SessionStart-хук hello. Мак ещё не привязан —
плагин сам просит привязку на почту Claude (MEMORYAI_EMAIL, иначе почта входа в Claude Code из
~/.claude.json), на телефон уходит пуш «Claude Code просит доступ к Memory AI», в сессии —
systemMessage с числом. Подтверждение ждёт фоновый connect-bg, после него сессия появляется в
«Чатах». Отказ, чужая почта, нет сети — пауза в ~/.memory-ai/auto.json, а не вопрос в каждой
сессии. /memory-ai:disconnect выключает автоподключение, /memory-ai:connect <почта> — ручное.

Только стандартная библиотека и Python 3.9 (/usr/bin/python3 на macOS).
Команды: connect start <почта> | connect wait | status | disconnect | hook <событие>.
"""
import base64
import copy
import datetime
import fcntl
import hashlib
import json
import mimetypes
import os
import re
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

VERSION = "0.3.9"
BASE = os.environ.get("MEMORYAI_BASE", "https://memory.memoryai.club/api:0Zk38beL").rstrip("/")
HOME = os.path.expanduser(os.environ.get("MEMORYAI_HOME", "~/.memory-ai"))
CONF = os.path.join(HOME, "claude-code.json")
PENDING = os.path.join(HOME, "pairing.json")
LISTEN = os.path.join(HOME, "listen")       # <session_id>.json — сессия простаивает и ждёт ответа
READY = os.path.join(HOME, "ready")         # <session_id> — для неё что-то пришло, забрать /cc/take
SYNC = os.path.join(HOME, "sync")           # <session_id>.json — докуда прочитан transcript; .lock .again .act
STOPS = os.path.join(HOME, "stop")          # <session_id> — с телефона нажали «Стоп»
INBOX = os.path.join(HOME, "inbox")         # <начало session_id>/ — файлы, присланные с телефона
QUEUE = os.path.join(HOME, "queue")         # <session_id> — с телефона пишут, пока Claude работает
PERMS = os.path.join(HOME, "perm")          # <session_id>~<pid>.json — хук ждёт ответа о разрешении
LOG = os.path.join(HOME, "log.txt")
AUTO = os.path.join(HOME, "auto.json")      # автоподключение: {state, until, told} — когда пробовать снова

AWAY_AFTER = 120               # секунд без клавиатуры и мыши — человек не у Мака
BACK_AFTER = 3                 # тронул клавиатуру — вернулся (вопрос о разрешении уходит на Мак)
MAX_LISTEN = 11 * 3600         # ожидатель живёт до 11 часов (таймаут хука в hooks.json — 12)
PERM_WAIT = 600                # столько ждём «да / нет» с телефона (как PERM_TTL на сервере)
ASK_AFTER = 30                 # Claude Desktop, VS Code: вопрос уже на экране Мака; Мак не трогали столько — и на телефон
PERM_RETRY = 3                 # сервер не ответил (VPN, обрыв) — спросить телефон снова через столько секунд
# Ответ — выбор варианта, а не «да / нет»: на телефоне таких кнопок пока нет, вопрос остаётся на Маке.
ASK_TOOLS = {"AskUserQuestion"}
MAX_TEXT = 30000
FILE_MAX = 26 * 1024 * 1024    # файл с телефона (на сервере лимит 25 МБ)
INBOX_DAYS = 14                # столько хранятся папки inbox
FROM_PHONE = "📱"
PERM_TOOLS_TEXT = {"Bash": "command", "Edit": "file_path", "Write": "file_path", "MultiEdit": "file_path",
                   "NotebookEdit": "notebook_path", "Read": "file_path", "WebFetch": "url", "WebSearch": "query",
                   "ExitPlanMode": "plan"}


# ---------------------------------------------------------------- общее

def _mkdirs():
    for d in (HOME, LISTEN, READY, SYNC, STOPS, QUEUE, PERMS):
        os.makedirs(d, mode=0o700, exist_ok=True)


def log(msg):
    try:
        _mkdirs()
        if os.path.exists(LOG) and os.path.getsize(LOG) > 256 * 1024:
            os.replace(LOG, LOG + ".1")
        with open(LOG, "a") as f:
            f.write(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} [{os.getpid()}] {msg}\n")
    except OSError:
        pass


def read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def write_json(path, data, mode=0o600):
    _mkdirs()
    tmp = f"{path}.{os.getpid()}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    with os.fdopen(fd, "w") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def conf():
    c = read_json(CONF)
    return c if c and c.get("token") else None


def device_id():
    """Постоянный id этой установки: тот же Мак при повторной привязке — тот же аккаунт."""
    path = os.path.join(HOME, "device-id")
    try:
        with open(path) as f:
            v = f.read().strip()
        if re.fullmatch(r"[A-Za-z0-9_-]{8,64}", v):
            return v
    except OSError:
        pass
    _mkdirs()
    v = "mac-" + secrets.token_urlsafe(16)
    with open(path, "w") as f:
        f.write(v)
    return v


def mac_name():
    try:
        out = subprocess.run(["scutil", "--get", "ComputerName"], capture_output=True, text=True, timeout=3).stdout
        return out.strip()[:40] or "Mac"
    except (OSError, subprocess.SubprocessError):
        return "Mac"


def idle_seconds():
    """Сколько секунд не трогали клавиатуру и мышь (HIDIdleTime). Нет данных — 0 (у Мака)."""
    force = os.environ.get("MEMORYAI_IDLE")      # для тестов: число секунд или @файл с числом
    if force is not None:
        try:
            if force.startswith("@"):
                with open(force[1:]) as f:
                    force = f.read()
            return float(force)
        except (OSError, ValueError):
            return 0.0
    try:
        out = subprocess.run(["ioreg", "-c", "IOHIDSystem", "-d", "4"], capture_output=True, text=True,
                             timeout=3).stdout
        m = re.search(r'"HIDIdleTime" = (\d+)', out)
        return int(m.group(1)) / 1e9 if m else 0.0
    except (OSError, subprocess.SubprocessError):
        return 0.0


def away():
    return idle_seconds() >= AWAY_AFTER


def call(method, path, body=None, token=None, timeout=8):
    """→ (status, json). Сеть недоступна — (0, {})."""
    url = BASE + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("User-Agent", f"memory-ai-claude-plugin/{VERSION}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except ValueError:
            return e.code, {}
    except (urllib.error.URLError, OSError, ValueError) as e:
        log(f"{method} {path}: {e}")
        return 0, {}


# Ключи и пароли не уезжают с Мака. Те же правила, что в ext_claude.py (сервер закрывает второй раз).
_SECRETS = [
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?(?:-----END [A-Z ]*PRIVATE KEY-----|\Z)", re.S),
    re.compile(r"\b(sk-ant-[A-Za-z0-9_-]{8})[A-Za-z0-9_-]{12,}"),
    re.compile(r"\b(sk-(?:proj-)?[A-Za-z0-9]{4})[A-Za-z0-9_-]{16,}"),
    re.compile(r"\b((?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{4})[A-Za-z0-9]{20,}"),
    re.compile(r"\b(github_pat_[A-Za-z0-9]{4})[A-Za-z0-9_]{20,}"),
    re.compile(r"\b(xox[abposr]-[A-Za-z0-9]{4})[A-Za-z0-9-]{10,}"),
    re.compile(r"\b(AKIA[0-9A-Z]{4})[0-9A-Z]{12}\b"),
    re.compile(r"\b(AIza[0-9A-Za-z_-]{4})[0-9A-Za-z_-]{31}\b"),
    re.compile(r"\b(mai_[a-z]{1,4}_[A-Za-z0-9]{4})[A-Za-z0-9_-]{16,}"),
    re.compile(r"\b(\d{8,10}:AA[A-Za-z0-9_-]{2})[A-Za-z0-9_-]{25,}"),
    re.compile(r"\b(eyJ[A-Za-z0-9_-]{6})[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"),
    re.compile(r"(?i)\b((?:password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key)"
               r"[\"']?\s*[:=]\s*[\"']?)(?=[\w\-+/=!@#$%^&*]*\d)[\w\-+/=!@#$%^&*]{6,}"),
]


def mask(text):
    s = str(text or "")
    for rx in _SECRETS:
        s = rx.sub(lambda m: (m.group(1) if m.lastindex else "") + "•••", s)
    return s


def clip(text, cap=MAX_TEXT):
    s = mask(text)
    return s if len(s) <= cap else s[:cap] + "…"


def pid_alive(pid):
    try:
        os.kill(int(pid), 0)
        return True
    except (OSError, ValueError, TypeError):
        return False


def _sid_ok(sid):
    return bool(re.fullmatch(r"[A-Za-z0-9_.-]{6,100}", str(sid or "")))


def _rm(path):
    try:
        os.remove(path)
    except OSError:
        pass


def stop_listening(sid):
    _rm(os.path.join(LISTEN, sid + ".json"))
    _rm(os.path.join(READY, sid))


# ---------------------------------------------------------------- привязка

PAIR_TTL = 900                 # запрос привязки живёт 15 минут (PAIR_TTL на сервере)
HOUR, DAY = 3600, 86400

# Тексты для человека: в терминале (команды) и в systemMessage сессии (автоподключение). ru / en.
_T = {
    "need_email": ("Нужна почта, с которой вы входите в Memory AI: /memory-ai:connect you@example.com",
                   "Memory AI needs the email you sign in with: /memory-ai:connect you@example.com"),
    "already": ("Этот Мак уже подключён к Memory AI как «{device}». Привязать заново: /memory-ai:disconnect, "
                "потом /memory-ai:connect.",
                "This Mac is already connected to Memory AI as “{device}”. To link it again: /memory-ai:disconnect, "
                "then /memory-ai:connect."),
    "not_on": ("Для почты {email} сессии Claude Code в «Чатах» Memory AI пока не включены.",
               "Claude Code sessions in Memory AI Chats aren't on for {email} yet."),
    "slow": ("Слишком много попыток привязки. Попробуйте через час.",
             "Too many link attempts. Try again in an hour."),
    "no_server": ("Сервер Memory AI не ответил ({st}). Попробуйте позже.",
                  "The Memory AI server didn't answer ({st}). Try again later."),
    "net": ("нет сети", "no network"),
    "pushed": ("На телефон ушёл запрос «Claude Code просит доступ к Memory AI». Откройте его и выберите число {n}.",
               "Your phone got a request: “Claude Code wants access to Memory AI”. Open it and pick {n}."),
    "open_app": ("Откройте Memory AI на телефоне: там запрос «Claude Code просит доступ». Выберите число {n}.",
                 "Open Memory AI on your phone: there's a request “Claude Code wants access”. Pick {n}."),
    "lives": ("Запрос живёт {m} минут.", "The request lasts {m} minutes."),
    "done": ("Готово: «{device}» подключён к Memory AI{who}. Сессии Claude Code с этого Мака появятся в «Чатах» "
             "с первого запроса.",
             "Done: “{device}” is connected to Memory AI{who}. Claude Code sessions from this Mac show up in Chats "
             "from the next prompt."),
    "denied": ("На телефоне отказали (или выбрали другое число). Можно начать заново: /memory-ai:connect <почта>.",
               "Declined on the phone (or a different number was picked). Start again: /memory-ai:connect <email>."),
    "expired": ("Запрос истёк. Начните заново: /memory-ai:connect <почта>.",
                "The request expired. Start again: /memory-ai:connect <email>."),
    "missing": ("Запрос не найден. Начните заново: /memory-ai:connect <почта>.",
                "The request wasn't found. Start again: /memory-ai:connect <email>."),
    "issued": ("Этот запрос уже использован. Начните заново: /memory-ai:connect <почта>.",
               "This request was already used. Start again: /memory-ai:connect <email>."),
    "not_started": ("Привязка не начата: сначала /memory-ai:connect <почта>.",
                    "Linking hasn't started: run /memory-ai:connect <email> first."),
    "still": ("Пока не подтвердили на телефоне. Подтвердите и запустите ожидание ещё раз: {cmd}",
              "Not confirmed on the phone yet. Confirm it there and run the wait again: {cmd}"),
    "connected": ("Подключено: «{device}». Сессий ждут ответа с телефона: {n}. Вы {where} (простой {idle} с).",
                  "Connected: “{device}”. Sessions waiting for a reply from the phone: {n}. You're {where} "
                  "(idle {idle} s)."),
    "at_mac": ("у Мака", "at the Mac"),
    "not_at_mac": ("не у Мака", "away from the Mac"),
    "not_connected": ("Не подключено. /memory-ai:connect <почта в Memory AI>",
                      "Not connected. /memory-ai:connect <your Memory AI email>"),
    "waiting": ("Ждём подтверждения на телефоне ({email}): выберите число {n}.",
                "Waiting for your phone ({email}): pick {n}."),
    "revoked": ("Привязка больше не действует (Мак убрали в приложении или выключили функцию). "
                "Подключить заново: /memory-ai:connect <почта>.",
                "The link no longer works (the Mac was removed in the app or the feature was turned off). "
                "Connect again: /memory-ai:connect <email>."),
    "bye": ("Этот Мак отключён от Memory AI и сам больше не подключится. Чаты сессий остаются в приложении, "
            "пока их не убрать там.",
            "This Mac is disconnected from Memory AI and won't reconnect on its own. Session chats stay in the app "
            "until you remove them there."),
    # автоподключение: systemMessage в начале сессии
    "auto_sent": ("Memory AI: на телефон ({email}) ушёл запрос «Claude Code просит доступ». Выберите в нём число {n}, "
                  "и сессии с этого Мака появятся в «Чатах». Другая почта: /memory-ai:connect <почта>",
                  "Memory AI: your phone ({email}) got a request “Claude Code wants access”. Pick {n} in it, and this "
                  "Mac's sessions show up in Chats. Different email: /memory-ai:connect <email>"),
    "auto_open": ("Memory AI: откройте приложение на телефоне ({email}), там запрос «Claude Code просит доступ». "
                  "Выберите число {n}, и сессии с этого Мака появятся в «Чатах». Другая почта: "
                  "/memory-ai:connect <почта>",
                  "Memory AI: open the app on your phone ({email}); there's a request “Claude Code wants access”. "
                  "Pick {n}, and this Mac's sessions show up in Chats. Different email: /memory-ai:connect <email>"),
    "auto_wait": ("Memory AI ждёт подтверждения на телефоне: выберите число {n}.",
                  "Memory AI is waiting for your phone: pick {n}."),
    "auto_no_email": ("Memory AI: чтобы сессии с этого Мака были в «Чатах», подключите его: "
                      "/memory-ai:connect <почта в Memory AI>",
                      "Memory AI: to see this Mac's sessions in Chats, connect it: "
                      "/memory-ai:connect <your Memory AI email>"),
    "auto_not_on": ("Memory AI: для {email} сессии Claude Code в «Чатах» не включены. Если входите в Memory AI "
                    "с другой почтой: /memory-ai:connect <почта>",
                    "Memory AI: Claude Code sessions in Chats aren't on for {email}. If you sign in to Memory AI "
                    "with another email: /memory-ai:connect <email>"),
    "auto_joined": ("Memory AI: этот Мак подключён ({device}). Сессии Claude Code видны в «Чатах».",
                    "Memory AI: this Mac is connected ({device}). Claude Code sessions are in Chats."),
}
_lang_cache = []


def lang():
    """ru или en: MEMORYAI_LANG, иначе первый язык системы, иначе LANG."""
    if not _lang_cache:
        v = os.environ.get("MEMORYAI_LANG") or ""
        if not v:
            try:
                out = subprocess.run(["defaults", "read", "-g", "AppleLanguages"], capture_output=True, text=True,
                                     timeout=2).stdout
                m = re.search(r"[A-Za-z]{2,3}", out)
                v = m.group(0) if m else ""
            except (OSError, subprocess.SubprocessError):
                v = ""
        v = v or os.environ.get("LANG") or ""
        _lang_cache.append("ru" if v.lower().startswith("ru") else "en")
    return _lang_cache[0]


def T(key, **kw):
    ru, en = _T[key]
    return (ru if lang() == "ru" else en).format(**kw)


def _spawn(*args):
    """Фоновый процесс плагина, отвязанный от хука: хук возвращается сразу."""
    try:
        subprocess.Popen([sys.executable, os.path.abspath(__file__), *args], start_new_session=True,
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except OSError as e:
        log(f"spawn {args[0]}: {e}")


def _fresh(p):
    return bool(p and p.get("pair") and float(p.get("until") or (p.get("at") or 0) + PAIR_TTL) > time.time())


def _drop_pending(p):
    """Убрать запрос, если он всё ещё тот же (человек мог начать новый)."""
    cur = read_json(PENDING)
    if cur and cur.get("pair") == p.get("pair"):
        _rm(PENDING)


def _pair_request(email, auto=False, timeout=8):
    """POST /cc/pair → (status, данные запроса или None). Запрос сохраняется в pairing.json."""
    verifier = secrets.token_urlsafe(32)
    name = mac_name()
    st, r = call("POST", "/cc/pair", {"email": email, "device_id": device_id(), "device_name": name,
                                      "challenge": hashlib.sha256(verifier.encode()).hexdigest()}, timeout=timeout)
    if st != 200 or not r.get("pair"):
        return st, None
    now = time.time()
    p = {"pair": r["pair"], "verifier": verifier, "email": email, "device": name, "at": now,
         "number": r.get("number"), "pushed": bool(r.get("pushed")), "auto": auto,
         "until": now + int(r.get("expires_in") or PAIR_TTL)}
    write_json(PENDING, p)
    return st, p


def _pair_check(p):
    """Один опрос запроса → (approved | pending | denied | expired | missing | issued | None, ответ).
    approved — токен сохранён, запрос убран."""
    st, r = call("POST", f"/cc/pair/{p['pair']}", {"verifier": p["verifier"], "version": VERSION})
    status = r.get("status") if st in (200, 404) else None
    if status == "approved" and r.get("token"):
        write_json(CONF, {"token": r["token"], "device": r.get("device") or p["device"], "email": p["email"],
                          "base": BASE, "at": time.time()})
        _drop_pending(p)
        _rm(AUTO)
    return status, r


def connect_start(email):
    email = (email or "").strip()
    if "@" not in email:
        print(T("need_email"))
        return 1
    c = conf()
    if c:
        st, me = call("GET", "/cc/me", token=c["token"])
        if st == 200:
            print(T("already", device=me.get("device")))
            return 0
    st, p = _pair_request(email)
    if st == 403:
        print(T("not_on", email=email))
        return 1
    if st == 429:
        print(T("slow"))
        return 1
    if not p:
        print(T("no_server", st=st or T("net")))
        return 1
    print(f"NUMBER: {p['number']}")
    print(T("pushed" if p["pushed"] else "open_app", n=p["number"]))
    print(T("lives", m=int(p["until"] - p["at"]) // 60))
    return 0


def connect_wait(limit=570):
    p = read_json(PENDING)
    if not p:
        print(T("not_started"))
        return 1
    end = time.time() + limit
    while time.time() < end:
        status, r = _pair_check(p)
        if status == "approved":
            who = f" ({r['name']})" if r.get("name") else ""
            print(T("done", device=r.get("device") or p["device"], who=who))
            return 0
        if status == "issued" and conf():      # токен забрал фоновый connect-bg этого же запроса
            print(T("done", device=conf().get("device") or p["device"], who=""))
            return 0
        if status in ("denied", "expired", "missing", "issued"):
            _drop_pending(p)
            print(T(status))
            return 1
        time.sleep(2)
    print(T("still", cmd=f"python3 \"{os.path.abspath(__file__)}\" connect wait"))
    return 2


def status_cmd():
    c = conf()
    if not c:
        p = read_json(PENDING)
        print(T("waiting", email=p.get("email"), n=p.get("number")) if _fresh(p) and p.get("number")
              else T("not_connected"))
        return 1
    st, me = call("GET", "/cc/me", token=c["token"])
    if st == 200:
        n = len([f for f in os.listdir(LISTEN) if f.endswith(".json")]) if os.path.isdir(LISTEN) else 0
        print(T("connected", device=me.get("device"), n=n, where=T("not_at_mac" if away() else "at_mac"),
                idle=int(idle_seconds())))
        return 0
    if st == 401:
        print(T("revoked"))
        return 1
    print(T("no_server", st=st or T("net")))
    return 1


def disconnect_cmd():
    c = conf()
    if c:
        call("POST", "/cc/bye", {}, token=c["token"])
    _rm(CONF)
    _rm(PENDING)
    write_json(AUTO, {"state": "off", "until": time.time() + 3650 * DAY})    # сам больше не подключается
    print(T("bye"))
    return 0


# ---------------------------------------------------------------- подключение без команд

def claude_email():
    """Почта для привязки: MEMORYAI_EMAIL, иначе почта входа в Claude Code (~/.claude.json, oauthAccount).
    Второе — недокументированное поле: нет его — просим /memory-ai:connect."""
    e = (os.environ.get("MEMORYAI_EMAIL") or "").strip()
    if "@" not in e:
        j = read_json(os.path.expanduser(os.environ.get("MEMORYAI_CLAUDE_JSON") or "~/.claude.json")) or {}
        acc = j.get("oauthAccount") if isinstance(j, dict) else None
        e = str((acc or {}).get("emailAddress") or "").strip() if isinstance(acc, dict) else ""
    return e if "@" in e and len(e) <= 200 and not re.search(r"\s", e) else None


def _say(text, context=None):
    """systemMessage видит человек; additionalContext — Claude (чтобы знал число, если спросят)."""
    out = {"systemMessage": text}
    if context:
        out["hookSpecificOutput"] = {"hookEventName": "SessionStart", "additionalContext": context}
    print(json.dumps(out, ensure_ascii=False))


def _pause(state, sec, **extra):
    write_json(AUTO, {"state": state, "until": time.time() + sec, **extra})


def hook_hello(inp):
    """SessionStart, синхронный. Привязан — молчит (раз после автопривязки говорит «подключён»).
    Нет — сам просит привязку на почту Claude и показывает число. Пауза в auto.json — молчит."""
    if os.environ.get("MEMORYAI_AUTO") == "0":
        return 0
    c = conf()
    a = read_json(AUTO) or {}
    if c:
        if a.get("state") == "joined":
            _rm(AUTO)
            _say(T("auto_joined", device=c.get("device") or "Mac"))
        return 0
    p = read_json(PENDING)
    if _fresh(p):
        if p.get("auto") and p.get("number"):
            _say(T("auto_wait", n=p["number"]))
        return 0
    if float(a.get("until") or 0) > time.time():
        return 0
    _mkdirs()
    fd = os.open(os.path.join(HOME, "auto.lock"), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return 0                                # соседняя сессия как раз просит привязку
        if conf() or _fresh(read_json(PENDING)):
            return 0
        a = read_json(AUTO) or {}
        if float(a.get("until") or 0) > time.time():
            return 0
        told = a.get("told")
        email = claude_email()
        if not email:
            _pause("no_email", 30 * DAY)
            _say(T("auto_no_email"))
            return 0
        st, p = _pair_request(email, auto=True, timeout=6)
        if st == 403:                               # чужая почта или функция не включена: раз сказать, раз в день пробовать
            _pause("not_on", DAY, told=email)
            if told != email:
                _say(T("auto_not_on", email=email))
            return 0
        if not p:                                   # нет сети, 429, 5xx — тихо, через час
            _pause("offline", HOUR, told=told)
            return 0
        sess = {k: inp.get(k) for k in ("session_id", "cwd", "transcript_path", "session_title")}
        _spawn("connect-bg", json.dumps({"pair": p["pair"], "session": sess}))
        _say(T("auto_sent" if p["pushed"] else "auto_open", email=email, n=p["number"]),
             context=(f"Memory AI plugin: this Mac is not linked to Memory AI yet. A link request went to the user's "
                      f"phone ({email}); they pick the number {p['number']} there. If they sign in to Memory AI with "
                      f"another email, they run /memory-ai:connect <email>."))
        log(f"auto: asked {email}")
        return 0
    finally:
        os.close(fd)


def connect_bg(arg):
    """Ждёт «да» на телефоне для запроса автоподключения. Да — токен, сессия, где спросили, — в «Чаты».
    Нет — пауза 30 дней, истёк — день. Человек начал заново или отключил — тихо выходит."""
    a = json.loads(arg)
    _mkdirs()
    fd = os.open(os.path.join(HOME, "pair.lock"), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return
        end = time.time() + PAIR_TTL + 30
        while time.time() < end:
            p = read_json(PENDING)
            if not p or p.get("pair") != a.get("pair"):
                return
            status, r = _pair_check(p)
            if status == "approved":
                write_json(AUTO, {"state": "joined", "until": 0})
                log("auto: connected")
                c, s = conf(), a.get("session") or {}
                if c and _sid_ok(s.get("session_id")):
                    hook_start(c, s)
                return
            if status in ("denied", "expired", "missing", "issued"):
                _drop_pending(p)
                if not conf():
                    _pause(status, {"denied": 30 * DAY, "expired": DAY}.get(status, HOUR))
                log(f"auto: {status}")
                return
            time.sleep(3)
        p = read_json(PENDING)
        if p and p.get("pair") == a.get("pair"):
            _drop_pending(p)
            _pause("expired", DAY)
    finally:
        os.close(fd)


# ---------------------------------------------------------------- ход сессии из transcript

SYNC_V = 1                     # меняется формат состояния — сессии подтягивают хвост заново
BACKFILL_BYTES = 8 << 20       # хвост transcript для первого раза
BACKFILL_N = 50                # из него — последние 50 сообщений и групп шагов
FULL_AGAIN = 32 << 20          # отстали сильнее — снова хвост, а не всё подряд
MAX_FILE = 10 << 20            # файл больше — только имя (nginx пускает 25 МБ на запрос)
REQ_TEXT = 300_000             # пачка без файлов
REQ_FILES = 16 << 20           # пачка с файлами
BATCH_N = 60
RUN_LINES = 30                 # в группе шагов храним последние 30 строк
STOP_TTL = 600                 # «Стоп» с телефона действует 10 минут, потом флаг не в счёт
ACT_TTL = 600
STOP_REASON = ("The user pressed Stop for this session in the Memory AI app on their phone. Do not call any "
               "more tools. Reply in one or two short lines with what is done and what is left, then end your turn.")

_SYS_TAGS = ("system-reminder|task-notification|cross-session-message|side-sessions-event|ci-monitor-event|"
             "local-command-stdout|local-command-stderr|local-command-caveat|bash-stdout|bash-stderr|"
             "user-prompt-submit-hook|command-message|command-name|command-args|command-contents|bash-input|"
             "tools-used-before-summary|session-start-hook|new-diagnostics|ide_selection|ide_opened_file|"
             "user-memory-input|post-tool-use-hook|channel|artifact-view-context|artifact-content-authored-by-others")
_SYS_ONE = re.compile(r"<(?:" + _SYS_TAGS + r")\b[^<>]*/>")                  # <artifact-content-authored-by-others/>
_SYS_RX = re.compile(r"<(" + _SYS_TAGS + r")\b[^<>]*(?<!/)>.*?(?:</\1>|\Z)", re.S)
# Обёртки, которых ещё нет в списке (12.11.2026 на телефон приехал <artifact-view-context …>{json}
# перед вопросом): блок <имя-с-дефисом …>…</имя-с-дефисом> в самом начале, если после него есть
# текст, и одиночный <имя-с-дефисом/> в начале или в конце.
_TAG = r"[a-z][a-z0-9]*(?:[-_][a-z0-9]+)+"
_WRAP_HEAD = re.compile(r"\A\s*(?:<(" + _TAG + r")\b[^<>]*(?<!/)>.*?</\1>|<" + _TAG + r"\b[^<>]*/>)\s*", re.S)
_WRAP_TAIL = re.compile(r"\s*<" + _TAG + r"\b[^<>]*/>\s*\Z")
# Запуск задачи по расписанию вручную: <scheduled-task name="x" file="…/SKILL.md">весь промпт</…> → /x
_SCHED_RX = re.compile(r"<scheduled-task\b([^<>]*)>.*?(?:</scheduled-task>|\Z)", re.S)
_PHONE_ECHO = "the user replied from their phone"
_SKIP_TOOLS = {"ToolSearch", "mcp__ccd_session__mark_chapter"}
# Задачи в фоне — полоска «N задач» над полем ввода, как на Маке: команда с run_in_background,
# агент, монитор, workflow. Кончается task-notification со <status> или TaskStop.
_BG_TOOLS = {"Bash": "shell", "Agent": "agent", "Task": "agent", "Monitor": "monitor", "Workflow": "workflow"}
TASKS_MAX = 20
# Что сессия получила от Anthropic (0.3.6): токены, контекст, раздумья, усилие, версия Claude
# Code, ветка — на страницу сессии в приложении. Опубликованные артефакты (строки frame-link
# с адресом claude.ai/artifact) — туда же.
ARTS_MAX = 60
# Шаги (0.3.8, 12.11.2026, Ник: «всё делать»): у каждого шага время («Bash · 14 с»), у правки
# файла «+42 −7» и сам дифф по тапу; у сессии — все строки и файлы, которые она трогала.
DIFF_MAX = 1500                # дифф одной правки в строке шагов, дальше «…»
DIFF_RUN = 12000               # диффы на всю группу шагов: не влезло — у старых шагов только счёт
FILES_MAX = 40                 # файлов сессии на её странице
TALLY_SEC = 6                  # первый подсчёт по всему старому журналу — не дольше
_ART_URL = re.compile(r"^https://claude\.ai/(?:code/)?artifact/[A-Za-z0-9_-]{6,80}$")
# Синхронизация как в приложении Claude (0.3.9, 12.11.2026, Ник: «Иногда даже не вижу чего то
# того же, скринов например!»): пояснения по ходу («Теперь проверю тёмную тему.») Claude Code
# пишет блоком thinking с thinkingDisplay "updates" (пустые такие блоки — скрытые раздумья), а
# снимки симулятора, браузера и Read картинки приходят картинками в ответе инструмента.
IMG_RESULT = 4                 # картинок из одного ответа инструмента
_EXT = {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp", "image/gif": "gif", "image/heic": "heic"}


def _human(text):
    """Текст человека без системных вставок. Команды: /cmd args и ! shell."""
    s = str(text or "")
    m = re.search(r"<command-name>\s*(.*?)\s*</command-name>", s, re.S)
    if m:
        a = re.search(r"<command-args>(.*?)</command-args>", s, re.S)
        cmd = m.group(1).strip()
        cmd = cmd if cmd.startswith("/") else "/" + cmd
        return (cmd + " " + (a.group(1).strip() if a else "")).strip()
    m = re.search(r"<bash-input>(.*?)</bash-input>", s, re.S)
    if m:
        return "! " + m.group(1).strip()
    if re.match(r"\s*Another Claude session sent a message:", s):
        return ""                              # сообщение другой сессии, не человек
    s = _unwrap(s)
    s = re.sub(r"\[Image #\d+\]", "", s)
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _unwrap(s):
    """Снять системные обёртки вокруг текста человека; вставленный текст (pasted_content) остаётся."""
    s = _SCHED_RX.sub(lambda m: _sched_cmd(m.group(1)), str(s or ""))
    s = _SYS_ONE.sub("", s)
    s = _SYS_RX.sub("", s)
    s = re.sub(r"</?pasted_content\b[^>]*>", "", s)
    while True:
        m = _WRAP_HEAD.match(s)
        if not m or not s[m.end():].strip():
            break
        s = s[m.end():]
    m = _WRAP_TAIL.search(s)
    if m and s[:m.start()].strip():
        s = s[:m.start()]
    return s


def _sched_cmd(attrs):
    m = re.search(r'\bname="([^"]{1,80})"', attrs or "")
    return "/" + m.group(1).strip() if m else ""


def _norm(s):
    return " ".join(str(s or "").split())[:400]


def _model_name(m):
    s = re.sub(r"\[.*?\]$", "", str(m or "").strip())
    if not s or s.startswith("<"):
        return None
    mm = re.match(r"claude-([a-z]+)-(\d+)(?:-(\d{1,2}))?(?:-\d{8})?$", s)
    if mm:
        return f"{mm.group(1).title()} {mm.group(2)}" + (f".{mm.group(3)}" if mm.group(3) else "")
    mm = re.match(r"claude-(\d+)(?:-(\d))?-([a-z]+)", s)
    if mm:
        return f"{mm.group(3).title()} {mm.group(1)}" + (f".{mm.group(2)}" if mm.group(2) else "")
    return s[:40]


def _first_line(v, cap=160):
    s = str(v or "").strip()
    return s.splitlines()[0][:cap] if s else ""


# id сессии или UUID вместо описания шага.
_RAW_ID = re.compile(r"^(local_)?[0-9a-f]{8}(-[0-9a-f]{1,12})*$")


def tool_line(name, ti):
    """Строка шага: «Инструмент<TAB>что делает»."""
    name = str(name or "")
    short = name.split("__")[-1] if name.startswith("mcp__") else name
    ti = ti if isinstance(ti, dict) else {}
    if name == "Bash":
        d = ti.get("description") or _first_line(ti.get("command"))
    elif name in ("Read", "Edit", "Write", "MultiEdit", "NotebookEdit"):
        d = os.path.basename(str(ti.get("file_path") or ti.get("notebook_path") or ""))
    elif name in ("Grep", "Glob"):
        d = ti.get("pattern")
    elif name == "WebFetch":
        d = ti.get("url")
    elif name == "WebSearch":
        d = ti.get("query")
    elif name in ("Agent", "Task"):
        d = ti.get("description")
    elif name == "Skill":
        d = ti.get("skill")
    elif name == "SendMessage":
        # Адрес (local_…) человеку ничего не говорит: первая строка самого сообщения.
        d = ti.get("summary") or _first_line(ti.get("message"))
    else:
        d = next((ti.get(k) for k in ("description", "action", "query", "url", "title", "name", "to")
                  if isinstance(ti.get(k), str) and ti.get(k).strip() and not _RAW_ID.match(ti.get(k).strip())), "")
    return mask(f"{short}\t{_first_line(d)}").strip()[:220]


def _img_file(b, i):
    src = b.get("source") or {}
    if src.get("type") != "base64" or not src.get("data"):
        return None
    mime = str(src.get("media_type") or "image/png")
    name = f"image-{i}.{_EXT.get(mime, 'png')}"
    data = str(src["data"])
    if len(data) * 3 // 4 > MAX_FILE:
        return {"name": name, "mime": mime, "big": True}
    return {"name": name, "mime": mime, "b64": data}


def _turn_start(d):
    """Строка человека (или уведомления), с которой начинается ход, а не ответ инструмента."""
    if d.get("type") != "user" or d.get("isSidechain") or d.get("isMeta") or d.get("isCompactSummary"):
        return False
    c = (d.get("message") or {}).get("content")
    return not (isinstance(c, list) and any(isinstance(b, dict) and b.get("type") == "tool_result" for b in c))


def _ms_between(a, b):
    try:
        ta = datetime.datetime.fromisoformat(str(a).replace("Z", "+00:00"))
        tb = datetime.datetime.fromisoformat(str(b).replace("Z", "+00:00"))
    except ValueError:
        return None
    ms = int((tb - ta).total_seconds() * 1000)
    return ms if 0 <= ms < 7 * 86400 * 1000 else None


def _rel(path, cwd):
    path = str(path or "")
    if cwd and path.startswith(str(cwd).rstrip("/") + "/"):
        return path[len(str(cwd).rstrip("/")) + 1:]
    home = os.path.expanduser("~")
    return "~" + path[len(home):] if path.startswith(home + "/") else path


def _edit(r, cwd):
    """toolUseResult правки файла → {f: путь, a: +строк, d: −строк, x: дифф} или None."""
    if not isinstance(r, dict) or not r.get("filePath"):
        return None
    hunks = r.get("structuredPatch") if isinstance(r.get("structuredPatch"), list) else []
    out = []
    a = dl = 0
    for h in hunks:
        if not isinstance(h, dict) or not isinstance(h.get("lines"), list):
            continue
        out.append(f"@@ -{h.get('oldStart', 0)},{h.get('oldLines', 0)} +{h.get('newStart', 0)},{h.get('newLines', 0)} @@")
        for ln in h["lines"]:
            ln = str(ln)
            a += ln.startswith("+")
            dl += ln.startswith("-")
            out.append(ln)
    if not hunks and r.get("type") == "create" and isinstance(r.get("content"), str):
        lines = r["content"].split("\n")
        if lines and lines[-1] == "":
            lines.pop()
        a = len(lines)
        out = ["+" + ln for ln in lines]
    elif not hunks:
        return None
    x, n = [], 0
    for ln in out:
        ln = ln[:300]
        if n + len(ln) + 1 > DIFF_MAX:
            x.append("…")
            break
        x.append(ln)
        n += len(ln) + 1
    return {"f": mask(_rel(r["filePath"], cwd))[:200], "a": a, "d": dl, "x": mask("\n".join(x))}


class Parser:
    """Строки transcript → сообщения для «Чатов». Состояние (группа шагов, план, модель, имя)
    переживает запуски: лежит в sync/<sid>.json вместе со смещением."""

    def __init__(self, st, cwd=None):
        self.run = st.get("run")
        self.plan = st.get("plan")
        self.model = st.get("model")
        self.title = st.get("title")
        self.recent = st.get("recent") or []     # [норм. текст, ts] последних сообщений человека
        self.tasks = st.get("tasks") or []       # [{id: tool_use, tid, k, t, ts, fg?}] — задачи в фоне
        self.use = st.get("use")                 # токены сессии; None — старый журнал ещё не считали
        self.info = st.get("info") or {}         # усилие, версия Claude Code, ветка, режим разрешений
        self.arts = st.get("arts") or []         # [{u, t, k, ts}] — опубликованные артефакты
        self.arts_dirty = bool(st.get("artsd"))  # появился новый — слать список, пока сервер не примет
        self.files = st.get("files") or []       # [[путь, +, −]] — файлы, которые правила сессия, новые в конце
        self.cwd = cwd

    def state(self):
        return copy.deepcopy({"run": self.run, "plan": self.plan, "model": self.model, "title": self.title,
                              "recent": self.recent[-6:], "tasks": self.tasks[-TASKS_MAX:],
                              "use": self.use, "info": self.info, "arts": self.arts[-ARTS_MAX:],
                              "artsd": self.arts_dirty, "files": self.files[-FILES_MAX:]})

    # -- токены, усилие, артефакты
    def stats(self, d):
        """Любая строка журнала: что сессия получила от Anthropic и что опубликовала."""
        t = d.get("type")
        if t == "frame-link":
            self._art(d)
            return
        if t not in ("assistant", "user"):
            return
        for k, src in (("ver", "version"), ("br", "gitBranch"), ("mode", "permissionMode")):
            v = d.get(src)
            if isinstance(v, str) and v:
                self.info[k] = v[:60]
        if t != "assistant":
            self._edit_stats(d)
            if self.use is not None and _turn_start(d):
                self.use["tout"] = 0                 # новый ход: «↓ N» у таймера считается с нуля
            return
        if isinstance(d.get("effort"), str) and d["effort"]:
            self.info["eff"] = d["effort"][:20]
        msg = d.get("message") or {}
        u = msg.get("usage")
        if not isinstance(u, dict):
            return
        use = self.use if self.use is not None else {}
        self.use = use
        ms = d.get("thinkingDurationMs")
        if isinstance(ms, (int, float)) and ms > 0:
            use["ms"] = int(use.get("ms") or 0) + int(ms)
        # Одно сообщение API лежит в журнале несколькими строками (по блоку) с тем же usage.
        mid = str(msg.get("id") or d.get("requestId") or "")[:80]
        ids = use.get("ids") or []
        if mid and mid in ids:
            return
        use["ids"] = (ids + [mid])[-8:]

        def n(v):
            return int(v) if isinstance(v, (int, float)) and v > 0 else 0

        inp, cr, cw = n(u.get("input_tokens")), n(u.get("cache_read_input_tokens")), n(u.get("cache_creation_input_tokens"))
        det = u.get("output_tokens_details") if isinstance(u.get("output_tokens_details"), dict) else {}
        web = u.get("server_tool_use") if isinstance(u.get("server_tool_use"), dict) else {}
        for k, v in (("inp", inp), ("cr", cr), ("cw", cw), ("out", n(u.get("output_tokens"))),
                     ("tout", n(u.get("output_tokens"))), ("think", n(det.get("thinking_tokens"))), ("calls", 1),
                     ("web", n(web.get("web_search_requests")) + n(web.get("web_fetch_requests")))):
            use[k] = int(use.get(k) or 0) + v
        if not d.get("isSidechain"):
            use["ctx"] = inp + cr + cw           # сколько Claude держит в контексте сейчас
        if not use.get("since"):
            use["since"] = d.get("timestamp")

    def _edit_stats(self, d):
        """Правка файла (Edit / Write / MultiEdit): строки «+ −» в счёт сессии и в список файлов."""
        e = _edit(d.get("toolUseResult"), d.get("cwd") or self.cwd)
        if not e:
            return
        use = self.use if self.use is not None else {}
        self.use = use
        use["la"] = int(use.get("la") or 0) + e["a"]
        use["ld"] = int(use.get("ld") or 0) + e["d"]
        old = next((f for f in self.files if f[0] == e["f"]), None)
        row = [e["f"], e["a"] + (old[1] if old else 0), e["d"] + (old[2] if old else 0)]
        self.files = [f for f in self.files if f[0] != e["f"]][-(FILES_MAX - 1):] + [row]

    def _art(self, d):
        u = str(d.get("frameUrl") or "")
        if not _ART_URL.match(u):
            return
        path = str(d.get("path") or "")
        kind = os.path.splitext(path)[1].lstrip(".").lower()[:8] or "html"
        title = mask(_first_line(d.get("title") or os.path.splitext(os.path.basename(path))[0] or "Artifact", 120))
        a = {"u": u, "t": title, "k": kind, "ts": d.get("timestamp")}
        old = next((x for x in self.arts if x["u"] == u), None)
        if old and old.get("t") == a["t"]:
            old["ts"] = a["ts"]                  # тот же артефакт обновили
            self.arts = [x for x in self.arts if x is not old] + [old]
        else:
            self.arts = [x for x in self.arts if x["u"] != u] + [a]
        self.arts = self.arts[-ARTS_MAX:]
        self.arts_dirty = True

    # -- задачи в фоне
    def _task_use(self, d, b):
        name, ti = str(b.get("name") or ""), b.get("input") if isinstance(b.get("input"), dict) else {}
        if name in ("TaskStop", "KillShell"):
            tid = str(ti.get("task_id") or ti.get("shell_id") or "")
            self.tasks = [t for t in self.tasks if not tid or t.get("tid") != tid]
            return
        k = _BG_TOOLS.get(name)
        if not k or not b.get("id"):
            return
        if k == "workflow":
            m = re.search(r"\bname\s*:\s*['\"]([^'\"]+)", str(ti.get("script") or ""))
            title = (m.group(1) if m else None) or ti.get("name") or "Workflow"
        elif k == "shell":
            title = ti.get("description") or _first_line(ti.get("command"))
        else:
            title = ti.get("description") or name
        t = {"id": str(b["id"])[:80], "tid": None, "k": k, "t": mask(_first_line(title, 120)), "ts": d.get("timestamp")}
        if k == "shell" and not ti.get("run_in_background"):
            t["fg"] = True                       # обычная команда; в фон уйдёт, если её туда отправят
        self.tasks = [x for x in self.tasks if x["id"] != t["id"]] + [t]
        if len(self.tasks) > TASKS_MAX:          # сперва забываем обычные команды, потом самые старые
            fg = [x for x in self.tasks if x.get("fg")][: len(self.tasks) - TASKS_MAX]
            self.tasks = [x for x in self.tasks if x not in fg][-TASKS_MAX:]

    def _task_result(self, d, blocks):
        tr = d.get("toolUseResult") if isinstance(d.get("toolUseResult"), dict) else {}
        for b in blocks:
            if not (isinstance(b, dict) and b.get("type") == "tool_result"):
                continue
            t = next((x for x in self.tasks if x["id"] == str(b.get("tool_use_id") or "")[:80]), None)
            if not t:
                continue
            tid = tr.get("backgroundTaskId") or tr.get("taskId") or (
                tr.get("agentId") if tr.get("isAsync") or tr.get("status") == "async_launched" else None)
            if tid and not b.get("is_error"):
                t["tid"] = str(tid)[:80]
                t.pop("fg", None)
            else:
                self.tasks.remove(t)             # кончилась сразу или не ушла в фон

    def _task_done(self, text):
        text = str(text or "")
        if not re.search(r"<status>\s*\w", text):
            return                               # событие монитора: он ещё работает
        ids = set(re.findall(r"<task-id>\s*([^<\s]+)\s*</task-id>", text))
        uses = set(re.findall(r"<tool-use-id>\s*([^<\s]+)\s*</tool-use-id>", text))
        self.tasks = [t for t in self.tasks if t.get("tid") not in ids and t["id"] not in uses]

    # -- шаги и план
    def _step(self, d, uid, line, tid=None):
        if not self.run:
            self.run = {"id": "s:" + uid, "n": 0, "lines": [], "ts": d.get("timestamp")}
        r = self.run
        r["n"] += 1
        meta = (r.get("meta") or [None] * len(r["lines"])) + [None]
        r["lines"] = (r["lines"] + [line])[-RUN_LINES:]
        r["meta"] = meta[-len(r["lines"]):]
        if tid:
            pend = r.get("pend") or {}
            pend[str(tid)[:80]] = [r["n"], d.get("timestamp")]
            r["pend"] = dict(list(pend.items())[-RUN_LINES:])
        return self._run_item()

    def _run_item(self):
        r = self.run
        it = {"id": r["id"], "role": "assistant", "kind": "tool", "text": "\n".join(r["lines"]),
              "n": r["n"], "ts": r["ts"]}
        if any(r.get("meta") or []):
            it["meta"] = r["meta"]
        return it

    def _result(self, d, blocks):
        """Ответ инструмента: сколько шёл шаг, ошибка, у правки файла «+ −» и дифф."""
        r = self.run
        pend = (r or {}).get("pend") or {}
        res = [b for b in blocks if isinstance(b, dict) and b.get("type") == "tool_result"]
        hit = False
        for b in res:
            k = pend.pop(str(b.get("tool_use_id") or "")[:80], None)
            if not k:
                continue
            pos = k[0] - 1 - (r["n"] - len(r["lines"]))
            if not 0 <= pos < len(r["lines"]):
                continue
            m = {}
            ms = _ms_between(k[1], d.get("timestamp"))
            if ms is not None:
                m["ms"] = ms
            if b.get("is_error"):
                m["e"] = 1
            e = _edit(d.get("toolUseResult"), d.get("cwd") or self.cwd) if len(res) == 1 else None
            if e and not b.get("is_error"):
                m.update(e)
            r["meta"][pos] = m or None
            hit = True
        if not hit:
            return []
        left = DIFF_RUN                          # диффы новых шагов важнее: у старых остаётся счёт
        for m in reversed(r["meta"]):
            if m and m.get("x"):
                if len(m["x"]) > left:
                    m.pop("x")
                else:
                    left -= len(m["x"])
        return [self._run_item()]

    def _result_imgs(self, d, uid, blocks):
        """Картинки из ответа инструмента — фото в чате, как в приложении Claude. Шаги после снимка
        идут новой группой под ним; ждёт ответа ещё параллельный шаг — группа остаётся прежней."""
        imgs = [x for b in blocks if isinstance(b, dict) and b.get("type") == "tool_result"
                and isinstance(b.get("content"), list)
                for x in b["content"] if isinstance(x, dict) and x.get("type") == "image"]
        files = [f for f in (_img_file(x, i + 1) for i, x in enumerate(imgs[:IMG_RESULT])) if f]
        if not files:
            return []
        if not (self.run or {}).get("pend"):
            self._close()
        return [{"id": uid + ":img", "role": "assistant", "kind": "text", "text": "", "files": files,
                 "ts": d.get("timestamp")}]

    def _close(self):
        self.run = None

    def _plan(self, name, ti):
        ti = ti if isinstance(ti, dict) else {}
        if name == "TodoWrite" and isinstance(ti.get("todos"), list):
            self.plan = [{"t": mask(str(t.get("content") or ""))[:160], "s": str(t.get("status") or "pending")}
                         for t in ti["todos"][:30] if isinstance(t, dict)]
            return
        if name.endswith("__wrap_up"):
            if self.plan:
                for t in self.plan:
                    t["s"] = "completed"
            return
        if isinstance(ti.get("items"), list):
            self.plan = [{"t": mask(str(t.get("content") or ""))[:160], "s": str(t.get("status") or "pending")}
                         for t in ti["items"][:30] if isinstance(t, dict)]
            return
        p = self.plan or []
        if ti.get("advance"):
            cur = next((i for i, t in enumerate(p) if t["s"] == "in_progress"), None)
            if cur is not None:
                p[cur]["s"] = "completed"
            nxt = next((i for i, t in enumerate(p) if t["s"] == "pending"), None)
            if nxt is not None:
                p[nxt]["s"] = "in_progress"
        for n in ti.get("done") or []:
            if isinstance(n, int) and 1 <= n <= len(p):
                p[n - 1]["s"] = "completed"
        n = ti.get("start")
        if isinstance(n, int) and 1 <= n <= len(p):
            for t in p:
                if t["s"] == "in_progress":
                    t["s"] = "completed"
            p[n - 1]["s"] = "in_progress"

    # -- человек
    def _human_item(self, d, uid, text, blocks):
        text = _human(text)
        files = [f for f in (_img_file(b, i + 1) for i, b in enumerate(blocks)) if f]
        if not text and not files:
            return []
        if _PHONE_ECHO in text:
            return []                                     # ответ с телефона: он уже есть в чате
        key, ts = _norm(text), str(d.get("timestamp") or "")
        if key and any(k == key and _near(t, ts, 180) for k, t in self.recent):
            return []                                     # тот же текст из очереди и из запроса
        self.recent = (self.recent + [[key, ts]])[-6:]
        self._close()
        it = {"id": uid, "role": "user", "kind": "text", "text": clip(text), "ts": d.get("timestamp")}
        if files:
            it["files"] = files
        return [it]

    def _user(self, d, uid):
        if d.get("isSidechain") or d.get("isVisibleInTranscriptOnly") or d.get("isCompactSummary"):
            return []
        content = (d.get("message") or {}).get("content")
        blocks = content if isinstance(content, list) else []
        if self.tasks:
            self._task_result(d, blocks)
        text = content if isinstance(content, str) else "\n".join(
            str(b.get("text") or "") for b in blocks if isinstance(b, dict) and b.get("type") == "text")
        if text.lstrip().startswith("[Request interrupted by user"):
            self._close()
            return [{"id": uid, "role": "user", "kind": "note", "note": "interrupted", "text": "",
                     "ts": d.get("timestamp")}]
        origin = (d.get("origin") or {}).get("kind")
        if origin == "task-notification":
            return self._task_note(d, uid, text)
        if d.get("isMeta") or (origin and origin != "human") or d.get("promptSource") == "system":
            return []
        if any(isinstance(b, dict) and b.get("type") == "tool_result" for b in blocks):
            return (self._result(d, blocks) if self.run else []) + self._result_imgs(d, uid, blocks)
        if text.startswith("This session is being continued from a previous conversation"):
            return []
        imgs = [b for b in blocks if isinstance(b, dict) and b.get("type") == "image"]
        return self._human_item(d, uid, text, imgs)

    def _task_note(self, d, uid, text):
        self._task_done(text)
        m = re.search(r"<summary>(.*?)</summary>", str(text or ""), re.S)
        if not m or re.search(r"\bhook feedback\b", m.group(1), re.I):
            return []        # «Stop hook feedback»: так Claude Code приносит ответ с телефона, он уже в чате
        return [self._step(d, uid, mask("Task\t" + _first_line(m.group(1))))]

    def _attachment(self, d, uid):
        a = d.get("attachment") or {}
        t = a.get("type")
        if t == "model":
            self.model = (a.get("identity") or {}).get("marketingName") or _model_name(
                (a.get("identity") or {}).get("modelId")) or self.model
            return []
        if t != "queued_command" or a.get("isMeta") or d.get("isSidechain"):
            return []
        origin = (a.get("origin") or {}).get("kind")
        prompt = a.get("prompt")
        if origin == "task-notification":
            return self._task_note(d, uid, prompt if isinstance(prompt, str) else "")
        if origin != "human":
            return []
        blocks = prompt if isinstance(prompt, list) else []
        text = prompt if isinstance(prompt, str) else "\n".join(
            str(b.get("text") or "") for b in blocks if isinstance(b, dict) and b.get("type") == "text")
        imgs = [b for b in blocks if isinstance(b, dict) and b.get("type") == "image"]
        return self._human_item(d, uid, text, imgs)

    # -- Claude
    def _assistant(self, d, uid):
        if d.get("isSidechain"):
            return []
        msg = d.get("message") or {}
        name = _model_name(msg.get("model"))
        if name:
            self.model = name
        out = []
        blocks = msg.get("content") if isinstance(msg.get("content"), list) else []
        for i, b in enumerate(blocks):
            if not isinstance(b, dict):
                continue
            bid = uid if i == 0 else f"{uid}:{i}"
            if b.get("type") == "text":
                text = str(b.get("text") or "").strip()
                if not text or text == "No response requested.":
                    continue
                self._close()
                if d.get("isApiErrorMessage"):
                    out.append({"id": bid, "role": "assistant", "kind": "note", "note": "error",
                                "text": clip(text, 300), "ts": d.get("timestamp")})
                else:
                    out.append({"id": bid, "role": "assistant", "kind": "text", "text": clip(text),
                                "ts": d.get("timestamp")})
            elif b.get("type") == "thinking" and d.get("thinkingDisplay") == "updates":
                text = str(b.get("thinking") or "").strip()      # пояснение по ходу, см. IMG_RESULT
                if not text:
                    continue
                self._close()
                out.append({"id": bid, "role": "assistant", "kind": "text", "text": clip(text),
                            "ts": d.get("timestamp")})
            elif b.get("type") == "tool_use":
                tname, ti = str(b.get("name") or ""), b.get("input")
                self._task_use(d, b)
                if tname in _SKIP_TOOLS:
                    continue
                if tname == "TodoWrite" or tname.endswith("__update_plan") or tname.endswith("__wrap_up"):
                    self._plan(tname, ti)
                    continue
                if tname == "SendUserFile" and isinstance(ti, dict) and isinstance(ti.get("files"), list):
                    self._close()
                    out.append({"id": bid, "role": "assistant", "kind": "text", "ts": d.get("timestamp"),
                                "text": clip(ti.get("caption") or "", 2000),
                                "_paths": [str(p) for p in ti["files"][:10]], "_cwd": d.get("cwd") or self.cwd})
                    continue
                out.append(self._step(d, bid, tool_line(tname, ti), tid=b.get("id")))
        return out

    def feed(self, d):
        self.stats(d)
        uid = str(d.get("uuid") or "")[:80]
        t = d.get("type")
        if t == "custom-title" and d.get("customTitle"):
            self.title = str(d["customTitle"])[:120]
            return []
        if not uid:
            return []
        if t == "user":
            return self._user(d, uid)
        if t == "assistant":
            return self._assistant(d, uid)
        if t == "attachment":
            return self._attachment(d, uid)
        if t == "system" and d.get("subtype") == "compact_boundary":
            self._close()
            return [{"id": uid, "role": "assistant", "kind": "note", "note": "compact", "text": "",
                     "ts": d.get("timestamp")}]
        return []


def _near(a, b, sec):
    try:
        ta = datetime.datetime.fromisoformat(str(a).replace("Z", "+00:00"))
        tb = datetime.datetime.fromisoformat(str(b).replace("Z", "+00:00"))
        return abs((ta - tb).total_seconds()) <= sec
    except ValueError:
        return False


# Ключи и пароли не уезжают с Мака даже по SendUserFile: в чате — только имя.
_PRIVATE = re.compile(r"(^|/)(\.ssh|\.aws|\.gnupg|\.kube|\.docker|\.config/gh|Keychains)/|(^|/)\.env($|\.)|"
                      r"(^|/)\.netrc$|(^|/)\.npmrc$|(^|/)\.pypirc$|\.(pem|key|p12|pfx|keychain-db)$|(^|/)id_(rsa|dsa|ecdsa|ed25519)")


def _path_files(paths, cwd):
    out = []
    for p in paths:
        full = p if os.path.isabs(p) else os.path.join(cwd or "", p)
        name = os.path.basename(p)[:200] or "file"
        mime = mimetypes.guess_type(name)[0] or "application/octet-stream"
        if _PRIVATE.search(os.path.realpath(os.path.expanduser(full))):
            out.append({"name": name, "mime": mime, "big": True})
            continue
        try:
            size = os.path.getsize(full)
            if size > MAX_FILE:
                out.append({"name": name, "mime": mime, "bytes": size, "big": True})
                continue
            with open(full, "rb") as f:
                fl = {"name": name, "mime": mime, "b64": base64.b64encode(f.read()).decode()}
            if mime.startswith("video/"):
                poster = _video_poster(full)
                if poster:
                    fl["poster"] = poster
            out.append(fl)
        except OSError:
            out.append({"name": name, "mime": mime, "big": True})
    return out


def _video_poster(path):
    """Кадр ролика JPEG base64 — обложка видео в чате, как в Telegram. На сервере ffmpeg нет,
    а в macOS кадр отдаёт Quick Look (qlmanage). Нет его или не вышло — None: тогда
    приложение возьмёт кадр из самого ролика."""
    if not shutil.which("qlmanage"):
        return None
    tmp = tempfile.mkdtemp(prefix="memoryai-poster-")
    try:
        subprocess.run(["qlmanage", "-t", "-s", "640", "-o", tmp, path], capture_output=True, timeout=15)
        png = os.path.join(tmp, os.path.basename(path) + ".png")
        if not os.path.exists(png):
            return None
        out = png
        jpg = os.path.join(tmp, "poster.jpg")
        if shutil.which("sips") and subprocess.run(
                ["sips", "-s", "format", "jpeg", "-s", "formatOptions", "75", png, "--out", jpg],
                capture_output=True, timeout=15).returncode == 0 and os.path.exists(jpg):
            out = jpg
        with open(out, "rb") as f:
            raw = f.read()
        return base64.b64encode(raw).decode() if 0 < len(raw) <= 1024 * 1024 else None
    except (OSError, subprocess.SubprocessError):
        return None
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _ready(it):
    """Файлы SendUserFile читаются с диска только перед отправкой. Всего на сообщение — до
    MAX_FILE с хвостиком, остальное только именем."""
    if "_paths" in it:
        it = dict(it)
        it["files"] = _path_files(it.pop("_paths"), it.pop("_cwd", None))
    if it.get("files"):
        left = MAX_FILE * 4 // 3 + 4096
        files = []
        for fl in it["files"]:
            n = len(fl.get("b64") or "")
            if n > left:
                fl = {k: v for k, v in fl.items() if k not in ("b64", "poster")}
                fl["big"] = True
            left -= n if fl.get("b64") else 0
            files.append(fl)
        it = dict(it, files=files)
    return it


def _weight(it):
    return len(it.get("text") or "") + sum(len(f.get("b64") or "") + len(f.get("poster") or "")
                                           for f in it.get("files") or []) \
        + sum(len(m.get("x") or "") + 40 for m in it.get("meta") or [] if m) + 200


def _sync_post(c, inp, p, items, backfill, act, state=None, deadline=None):
    s = state or p.state()
    body = {"session_id": inp["session_id"], "cwd": inp.get("cwd"), "title": s["title"] or inp.get("session_title"),
            "model": s["model"], "plan": s["plan"], "backfill": backfill, "version": VERSION, "items": items,
            "tasks": [{k: t.get(k) for k in ("id", "k", "t", "ts")} for t in s.get("tasks") or [] if not t.get("fg")]}
    if act is not None:
        body["activity"] = act
    if p.use is not None or p.info:
        use = {k: v for k, v in (p.use or {}).items() if k != "ids"}
        body["usage"] = dict(use, **{k: v for k, v in p.info.items() if v})
        if p.files:
            body["usage"]["files"] = p.files[-FILES_MAX:]
    if p.arts_dirty:
        body["arts"] = p.arts[-ARTS_MAX:]
    tmo = 60 if any(i.get("files") for i in items) else 15
    if deadline:
        tmo = max(3, min(tmo, deadline + 5 - time.time()))   # Stop-хук не должен выйти за свой срок
    st, r = call("POST", "/cc/sync", body, token=c["token"], timeout=tmo)
    if st == 200 and r.get("stop"):
        _mkdirs()
        with open(os.path.join(STOPS, inp["session_id"]), "w") as f:
            f.write(str(time.time()))
    if st == 200:
        if r.get("queued"):                    # ответ с телефона ждёт: PostToolUse отдаст его после шага
            _mkdirs()
            open(os.path.join(QUEUE, inp["session_id"]), "w").close()
        else:
            _rm(os.path.join(QUEUE, inp["session_id"]))
    if st == 200 and "arts" in body and r.get("arts"):
        p.arts_dirty = False                   # сервер до 0.3.6 списка не хранит — пошлём ещё раз
    if st != 200:
        log(f"sync {inp['session_id'][:8]}: {st} {str(r)[:200]}")
    return st == 200


def _tally(p, f, end, deadline=None):
    """Токены и артефакты по журналу до места, откуда читаем ход (первый раз или после 0.3.5):
    только строки ответов Claude и frame-link, остальное мимо. Не дольше TALLY_SEC."""
    stop = time.time() + TALLY_SEC
    if deadline:
        stop = min(stop, deadline - 3)
    p.use = p.use or {}
    f.seek(0)
    pos = 0
    for line in f:
        if pos >= end:                           # строку, на которой начат хвост, ещё считаем здесь:
            break                                # разбор хвоста её пропускает как оборванную
        pos += len(line)
        if b'"frameUrl"' in line or (b'"usage"' in line and b'"assistant"' in line) or b'"structuredPatch"' in line:
            try:
                d = json.loads(line)
            except ValueError:
                continue
            if isinstance(d, dict):
                p.stats(d)
        if time.time() > stop:
            p.use["part"] = True                 # журнал огромный: посчитали не всё
            break
    p.arts_dirty = True
    if p.use:
        p.use["tout"] = 0                        # ход до хвоста не знаем: «↓ N» с нуля


def _batches(items):
    """Пачки по числу и весу; большой файл — один в пачке."""
    cur, w = [], 0
    for it in items:
        it = _ready(it)
        iw = _weight(it)
        cap = REQ_FILES if it.get("files") else REQ_TEXT
        if cur and (len(cur) >= BATCH_N or w + iw > cap):
            yield cur
            cur, w = [], 0
        cur.append(it)
        w += iw
    if cur:
        yield cur


def _activity(sid):
    a = read_json(os.path.join(SYNC, sid + ".act"))
    if a and time.time() - float(a.get("at") or 0) < ACT_TTL:
        return str(a.get("text") or "")[:220]
    return None


def sync(c, inp, deadline=None):
    """Дочитать transcript с запомненного места и отправить. Держать sync-замок сессии.
    → True — всё отправлено, False — нет (сеть, срок), None — нечего читать."""
    sid, tp = inp["session_id"], inp.get("transcript_path")
    if not tp or not os.path.isfile(tp):
        return None
    spath = os.path.join(SYNC, sid + ".json")
    st = read_json(spath) or {}
    stat = os.stat(tp)
    size = stat.st_size
    off = int(st.get("off") or 0)
    fresh = (st.get("v") != SYNC_V or st.get("ino") != stat.st_ino or off > size or size - off > FULL_AGAIN)
    if fresh:
        st = {"v": SYNC_V, "ino": stat.st_ino, "off": max(0, size - BACKFILL_BYTES)}
    p = Parser(st, inp.get("cwd"))
    act = _activity(sid)
    with open(tp, "rb") as f:
        if p.use is None and st["off"] > 0:
            _tally(p, f, st["off"], deadline)     # до хвоста; хвост посчитает разбор ниже
        f.seek(st["off"])
        if fresh and st["off"] > 0:
            f.readline()                                 # оборванная первая строка хвоста
        pos = f.tell()
        if fresh:
            got, order = {}, []
            for line in f:
                if not line.endswith(b"\n"):
                    break
                pos += len(line)
                for it in _parse_line(p, line):
                    if it["id"] not in got:
                        order.append(it["id"])
                    got[it["id"]] = it
            items = [got[i] for i in order[-BACKFILL_N:]]
            first = True
            for batch in _batches(items) if items else [[]]:
                if not _sync_post(c, inp, p, batch, first, act, deadline=deadline):
                    return False
                first = False
            write_json(spath, dict(p.state(), v=SYNC_V, ino=stat.st_ino, off=pos))
            return True
        batch, index, w, sent = [], {}, 0, False
        done_pos, done_state = pos, p.state()            # граница последней целой строки
        for line in f:
            if not line.endswith(b"\n"):
                break
            got = _parse_line(p, line)
            if any(it.get("_paths") or it.get("files") for it in got) and batch:
                if not _sync_post(c, inp, p, batch, False, act, done_state, deadline):   # файл едет отдельной пачкой
                    return False
                write_json(spath, dict(done_state, v=SYNC_V, ino=stat.st_ino, off=done_pos))
                batch, index, w, sent = [], {}, 0, True
            pos += len(line)
            for it in got:
                if it["id"] in index:
                    batch[index[it["id"]]] = it          # группа шагов выросла — та же строка
                    continue
                it = _ready(it)
                index[it["id"]] = len(batch)
                batch.append(it)
                w += _weight(it)
            done_pos, done_state = pos, p.state()
            if len(batch) >= BATCH_N or w > REQ_TEXT:
                if not _sync_post(c, inp, p, batch, False, act, deadline=deadline):
                    return False
                write_json(spath, dict(done_state, v=SYNC_V, ino=stat.st_ino, off=pos))
                batch, index, w, sent = [], {}, 0, True
                if deadline and time.time() > deadline:
                    return False
        if batch or not sent:
            if not _sync_post(c, inp, p, batch, False, act, deadline=deadline):
                return False
        write_json(spath, dict(p.state(), v=SYNC_V, ino=stat.st_ino, off=pos))
        return True


def _parse_line(p, line):
    try:
        d = json.loads(line)
    except ValueError:
        return []
    if not isinstance(d, dict) or d.get("type") in ("api-request", "api-request-blob", "api-request-shape"):
        return []
    try:
        return p.feed(d)
    except Exception as e:  # noqa: BLE001 — одна странная строка не ломает синк
        log(f"parse: {type(e).__name__}: {e}")
        return []


def _sync_lock(sid, wait=0.0):
    _mkdirs()
    fd = os.open(os.path.join(SYNC, sid + ".lock"), os.O_RDWR | os.O_CREAT, 0o600)
    end = time.time() + wait
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fd
        except OSError:
            if time.time() >= end:
                os.close(fd)
                return None
            time.sleep(0.2)


def sync_now(c, inp, wait=8.0):
    """Синк прямо в хуке (Stop): ждать замок до wait секунд, не успели — фоном."""
    end = time.time() + wait
    fd = _sync_lock(inp["session_id"], wait)
    if fd is None:
        spawn_sync(inp)
        return False
    try:
        ok = sync(c, inp, deadline=end)
    finally:
        os.close(fd)
    if ok is False:
        spawn_sync(inp)
    return ok


def spawn_sync(inp, delay=0):
    if not inp.get("transcript_path"):
        return
    arg = {"session_id": inp["session_id"], "cwd": inp.get("cwd"), "transcript_path": inp["transcript_path"],
           "session_title": inp.get("session_title"), "delay": delay}
    _spawn("sync-bg", json.dumps(arg))


def sync_bg(arg):
    """Фоновый синк. Один на сессию: занято — отметка «ещё раз», держатель замка дочитает."""
    c = conf()
    inp = json.loads(arg)
    sid = inp.get("session_id")
    if not c or not _sid_ok(sid) or os.environ.get("MEMORYAI_OFF") == "1":
        return
    if inp.get("delay"):
        time.sleep(min(float(inp["delay"]), 5))
    again = os.path.join(SYNC, sid + ".again")
    for _ in range(10):
        fd = _sync_lock(sid)
        if fd is None:
            _mkdirs()
            open(again, "w").close()
            return
        try:
            for _ in range(10):
                _rm(again)
                if sync(c, inp, deadline=time.time() + 90) is False or not os.path.exists(again):
                    break
        finally:
            os.close(fd)
        if not os.path.exists(again):
            return


def stop_flag(sid):
    """Нажат «Стоп» с телефона и флаг свежий."""
    path = os.path.join(STOPS, sid)
    try:
        if time.time() - os.path.getmtime(path) < STOP_TTL:
            return True
        _rm(path)
    except OSError:
        pass
    return False


# ---------------------------------------------------------------- хуки

def _event(c, inp, event, **extra):
    body = {"session_id": inp.get("session_id"), "event": event, "cwd": inp.get("cwd"),
            "title": inp.get("session_title"), "version": VERSION, **extra}
    st, r = call("POST", "/cc/event", body, token=c["token"])
    if st == 401:
        log(f"{event}: token rejected")
    return r if st == 200 else {}


def hook_start(c, inp):
    _event(c, inp, "start", away=away())
    spawn_sync(inp)


def hook_prompt(c, inp):
    """Сессия взялась за работу. Само сообщение (если это человек, а не системная вставка)
    приедет синком из transcript: туда оно попадает чуть позже хука."""
    sid = inp["session_id"]
    stop_listening(sid)                        # человек пишет с Мака — ожидатель больше не нужен
    _perm_settle(sid)
    text = str(inp.get("prompt") or "")
    human = bool(_human(text))
    if human:
        _rm(os.path.join(STOPS, sid))          # новый запрос человека — прежний «Стоп» не в счёт
    pid = inp.get("prompt_id") or hashlib.sha1((text + str(int(time.time() / 60))).encode()).hexdigest()[:20]
    _event(c, inp, "prompt", id=str(pid)[:80], synced=True, human=human, away=away())
    spawn_sync(inp, delay=2)


def hook_pre(c, inp):
    """PreToolUse: «Стоп» с телефона не пускает инструмент; ход и «что делает» — синком фоном."""
    sid = inp["session_id"]
    _perm_settle(sid, agent=str(inp.get("agent_id") or ""), older=3)   # следующий шаг — прежний вопрос решён
    if stop_flag(sid):
        print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny",
                                                 "permissionDecisionReason": STOP_REASON}}))
    else:
        tool = str(inp.get("tool_name") or "")
        if tool not in _SKIP_TOOLS:
            write_json(os.path.join(SYNC, sid + ".act"), {"text": tool_line(tool, inp.get("tool_input")),
                                                         "at": time.time()})
    spawn_sync(inp)


def hook_post(c, inp):
    """PostToolUse: человек написал с телефона, пока Claude работает, — отдать сразу после этого шага,
    как очередь в Claude Code на Маке, а не в конце хода. Без отметки синка — тихий выход.
    Нажат «Стоп» («Перебить» на телефоне) — не трогать: Stop-хук отдаст сообщение новым запросом."""
    sid = inp["session_id"]
    tool = str(inp.get("tool_name") or "")
    ti = inp.get("tool_input") if isinstance(inp.get("tool_input"), dict) else {}
    _perm_settle(sid, agent=str(inp.get("agent_id") or ""), key=_perm_key(tool, ti))   # разрешили на Маке
    flag = os.path.join(QUEUE, sid)
    if not os.path.exists(flag) or stop_flag(sid):
        return
    _rm(flag)
    st, r = call("POST", "/cc/take", {"session_id": sid}, token=c["token"], timeout=8)
    msgs = r.get("messages") if st == 200 else None
    if not msgs:
        return
    got = fetch_files(c, sid, r.get("files"), 20)
    print(json.dumps({"hookSpecificOutput": {"hookEventName": "PostToolUse",
                                             "additionalContext": _phone_text(msgs, got, now=True)}},
                     ensure_ascii=False))


def _phone_text(msgs, got=(), now=False):
    body = "\n\n".join(str(m) for m in msgs)
    if got:
        lines = [f"- {path}" if path else f"- {name} (could not download it, ask the user to send it again)"
                 for name, path in got]
        body += ("\n\nFiles the user attached on the phone, saved on this Mac (open them with Read):\n"
                 + "\n".join(lines))
    if now:
        return (f"{FROM_PHONE} Memory AI: while you were working, the user wrote from their phone (Memory AI app → "
                f"Chats). Read it now and take it into account in what you are doing; if it changes the task, "
                f"follow it:\n\n{body}")
    return (f"{FROM_PHONE} Memory AI: the user replied from their phone (Memory AI app → Chats). "
            f"Treat this as the user's next message in this session:\n\n{body}")


def _safe_name(name):
    name = os.path.basename(str(name or "").replace("\\", "/")).strip()
    name = re.sub(r"[\x00-\x1f:]", "_", name).lstrip(".")
    return name[:120] or "file"


def _download(c, f, folder, timeout):
    """Один файл с телефона → путь на Маке или None."""
    try:
        aid = int(f.get("id") or 0)
    except (TypeError, ValueError):
        return None
    if aid <= 0:
        return None
    req = urllib.request.Request(f"{BASE}/cc/file/{aid}")
    req.add_header("User-Agent", f"memory-ai-claude-plugin/{VERSION}")
    req.add_header("Authorization", "Bearer " + c["token"])
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = r.read(FILE_MAX + 1)
    except (urllib.error.URLError, OSError, ValueError) as e:
        log(f"file {aid}: {e}")
        return None
    if not data or len(data) > FILE_MAX:
        return None
    os.makedirs(folder, mode=0o700, exist_ok=True)
    name = _safe_name(f.get("name"))
    stem, ext = os.path.splitext(name)
    path, i = os.path.join(folder, name), 1
    while os.path.exists(path):
        path, i = os.path.join(folder, f"{stem} ({i}){ext}"), i + 1
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as out:
        out.write(data)
    return path


def fetch_files(c, sid, files, budget):
    """Файлы, присланные с телефона, → ~/.memory-ai/inbox/<сессия>/ → [(имя, путь или None)].
    budget — секунд на всё (Stop-хук ограничен таймаутом)."""
    if not isinstance(files, list) or not files:
        return []
    _prune_inbox()
    end = time.time() + budget
    folder = os.path.join(INBOX, re.sub(r"[^A-Za-z0-9_-]", "", sid)[:8] or "session")
    got = []
    for f in files[:20]:
        if not isinstance(f, dict):
            continue
        left = end - time.time()
        path = _download(c, f, folder, timeout=min(left, 60)) if left > 2 else None
        got.append((_safe_name(f.get("name")), path))
    return got


def _prune_inbox():
    """Папки inbox старше двух недель — прочь: их место на Маке, а файл есть в «Чатах»."""
    try:
        old = time.time() - INBOX_DAYS * 86400
        for d in os.listdir(INBOX):
            p = os.path.join(INBOX, d)
            if os.path.isdir(p) and os.path.getmtime(p) < old:
                for f in os.listdir(p):
                    _rm(os.path.join(p, f))
                os.rmdir(p)
    except OSError:
        pass


def hook_stop(c, inp):
    """Ход до конца → сервер, потом итог (пуш). Пришёл ответ с телефона — не останавливаться."""
    sid = inp["session_id"]
    _rm(os.path.join(SYNC, sid + ".act"))
    _perm_settle(sid)                          # ход кончился: сервер сам закроет карточки
    sync_now(c, inp, wait=8.0)
    stopped = stop_flag(sid)
    _rm(os.path.join(STOPS, sid))
    msg = str(inp.get("last_assistant_message") or "")
    eid = hashlib.sha1(f"{sid}|{inp.get('prompt_id')}|{msg}".encode()).hexdigest()[:20]
    r = _event(c, inp, "stop", id=eid, message=clip(msg), away=away(), synced=True, stopped=stopped)
    if r.get("deliver"):
        stop_listening(sid)
        _rm(os.path.join(QUEUE, sid))
        got = fetch_files(c, sid, r.get("files"), 25)
        print(json.dumps({"decision": "block", "reason": _phone_text(r["deliver"], got)}, ensure_ascii=False))
        return
    if r.get("ok"):
        write_json(os.path.join(LISTEN, sid + ".json"), {"gen": secrets.token_hex(8), "at": time.time(), "pid": None,
                                                         "cwd": inp.get("cwd")})


def hook_notify(c, inp):
    _event(c, inp, "notify", type=str(inp.get("notification_type") or ""), message=clip(inp.get("message"), 300),
           away=away())
    spawn_sync(inp)


def hook_end(c, inp):
    """SessionEnd: у хука полторы секунды — отправляет отдельный процесс."""
    stop_listening(inp["session_id"])
    _perm_settle(inp["session_id"])
    try:
        subprocess.Popen([sys.executable, os.path.abspath(__file__), "end-bg", json.dumps(
            {"session_id": inp["session_id"], "cwd": inp.get("cwd")})], start_new_session=True,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except OSError as e:
        log(f"end: {e}")


def end_bg(arg):
    c = conf()
    if c:
        _event(c, json.loads(arg), "end")


def _perm_title(tool, ti):
    key = PERM_TOOLS_TEXT.get(tool)
    if key and isinstance(ti, dict) and ti.get(key):
        line = str(ti[key]).strip().splitlines()[0][:200] if str(ti[key]).strip() else tool
        return line.lstrip("#").strip() or tool if tool == "ExitPlanMode" else line
    if isinstance(ti, dict) and ti.get("description"):
        return str(ti["description"])[:200]
    return tool


def _hosted():
    """Claude Code внутри приложения (Claude Desktop, VS Code, Agent SDK): диалог разрешения на Маке
    открыт одновременно с хуком, кто ответит первым — тот и решил. В терминале (cli) диалог
    появляется только после хука."""
    return (os.environ.get("CLAUDE_CODE_ENTRYPOINT") or "cli") != "cli"


def _perm_key(tool, ti):
    raw = json.dumps(ti, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(f"{tool}|{raw}".encode()).hexdigest()[:16]


def _perm_settle(sid, agent=None, key=None, older=0.0):
    """Вопрос о разрешении решён без телефона: на Маке ответили (инструмент пошёл — его key,
    начался следующий шаг того же агента — older), ход кончился. Ждущий хук видит, что его файла
    нет, и сам снимает карточку на телефоне; хук уже убит — снимаем фоном."""
    try:
        names = [n for n in os.listdir(PERMS) if n.startswith(sid + "~")]
    except OSError:
        return
    now = time.time()
    for n in names:
        path = os.path.join(PERMS, n)
        info = read_json(path) or {}
        if agent is not None and info.get("agent", "") != agent:
            continue
        if key is not None and info.get("key") != key:
            continue
        if older and now - float(info.get("at") or 0) < older:
            continue
        _rm(path)
        if info.get("id") and not pid_alive(info.get("pid")):
            _spawn("perm-cancel", str(info["id"]))


def hook_permission(c, inp):
    """Спросить телефон и ждать «да / нет».
    Терминал: диалог на Маке появится только после хука, поэтому телефон спрашиваем, лишь когда
    человек не у Мака, а вернулся — отдаём вопрос диалогу.
    Claude Desktop, VS Code: диалог уже на экране, хук идёт рядом, кто ответил первым — тот и решил.
    Телефон спрашиваем, если к Маку не притрагивались ASK_AFTER секунд, и ждём, даже если человек
    вернулся. Ответили на Маке — карточку снимает PostToolUse или следующий шаг (_perm_settle).
    Сервер не ответил (VPN, обрыв) — пробуем снова, пока не выйдет срок: вопрос не теряется."""
    sid = inp["session_id"]
    tool = str(inp.get("tool_name") or "")
    ti = inp.get("tool_input") if isinstance(inp.get("tool_input"), dict) else {}
    hosted = _hosted()
    tag = f"perm {sid[:8]} {tool}"
    idle = idle_seconds()
    if tool in ASK_TOOLS:
        log(f"{tag}: question with options, left to the Mac")
        return
    if not hosted and idle < AWAY_AFTER:
        log(f"{tag}: at the Mac ({int(idle)}s), terminal dialog")
        return
    _mkdirs()
    path = os.path.join(PERMS, f"{sid}~{os.getpid()}.json")
    me = {"at": time.time(), "tool": tool, "key": _perm_key(tool, ti), "agent": str(inp.get("agent_id") or ""),
          "pid": os.getpid()}
    write_json(path, me)

    def _quit(*_):
        raise SystemExit(0)                    # приложение решило само и гасит хук — снять карточку
    for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        try:
            signal.signal(sig, _quit)
        except (OSError, ValueError):
            pass
    wait = ASK_AFTER if hosted else AWAY_AFTER
    detail = (ti.get("command") if tool == "Bash" else ti.get("plan") if tool == "ExitPlanMode"
              else json.dumps(ti, ensure_ascii=False, indent=1))
    pid, decided, why, net = None, False, "mac", False
    end = time.time() + PERM_WAIT
    try:
        while time.time() < end:
            if not os.path.exists(path):
                log(f"{tag}: answered on the Mac")
                return
            if pid is None:
                idle = idle_seconds()
                if idle < wait:
                    time.sleep(2)
                    continue
                if not net:
                    spawn_sync(inp)            # на телефоне виден ход до вопроса
                st, r = call("POST", "/cc/perm", {"session_id": sid, "cwd": inp.get("cwd"),
                                                  "session_title": inp.get("session_title"), "tool": tool,
                                                  "title": mask(_perm_title(tool, ti)), "detail": clip(detail, 1500)},
                             token=c["token"])
                if st == 200 and r.get("off"):
                    log(f"{tag}: permissions from the phone are off for this Mac")
                    return
                if st == 200 and r.get("id"):
                    pid = r["id"]
                    end = min(end, time.time() + int(r.get("expires_in") or PERM_WAIT))
                    if not os.path.exists(path):
                        log(f"{tag}: answered on the Mac")
                        return
                    write_json(path, {**me, "id": pid})
                    log(f"{tag}: asked the phone (idle {int(idle)}s{', app' if hosted else ''})")
                    continue
                if st and st < 500 and st != 429:
                    log(f"{tag}: server said {st}")
                    return
                if not net:
                    log(f"{tag}: server unreachable ({st}), retrying")
                    net = True
                time.sleep(PERM_RETRY)
                continue
            time.sleep(2)
            if not hosted and idle_seconds() < BACK_AFTER:      # вернулся к Маку — решит в обычном диалоге
                log(f"{tag}: back at the Mac")
                return
            st, p = call("GET", f"/cc/perm/{pid}", token=c["token"])
            if st == 401 or st == 404:
                decided = True
                log(f"{tag}: server said {st}")
                return
            s = p.get("status") if st == 200 else None
            if s == "allow":
                decided = True
                d = {"behavior": "allow"}
                if tool == "ExitPlanMode":
                    d["updatedInput"] = ti                     # план одобрен как есть: инструменту нужен ввод
                print(json.dumps({"hookSpecificOutput": {"hookEventName": "PermissionRequest", "decision": d}},
                                 ensure_ascii=False))
                log(f"{tag}: allowed on the phone")
                return
            if s == "deny":
                decided = True
                note = (p.get("note") or "").strip()
                msg = (f"The user declined this from their phone and wrote: {note}" if note
                       else "The user declined this from their phone (Memory AI). Do not retry it; ask or do something else.")
                print(json.dumps({"hookSpecificOutput": {"hookEventName": "PermissionRequest",
                                                         "decision": {"behavior": "deny", "message": msg}}},
                                 ensure_ascii=False))
                log(f"{tag}: denied on the phone")
                return
            if s in ("mac", "expired"):
                decided = True
                log(f"{tag}: closed ({s})")
                return
        why = "timeout"
        log(f"{tag}: no answer in time")
    finally:
        _rm(path)
        if pid and not decided:
            call("POST", f"/cc/perm/{pid}/cancel", {"why": why}, token=c["token"], timeout=5)


# ---------------------------------------------------------------- ожидатель (asyncRewake)

def hook_listen(c, inp):
    """После Stop: ждать ответа с телефона и разбудить сессию. Выход 2 + stderr — Claude видит
    текст и продолжает; выход 0 — тихо (человек написал с Мака, сессия закрылась, срок вышел)."""
    sid = inp["session_id"]
    path = os.path.join(LISTEN, sid + ".json")
    start = time.time()
    me = None
    while time.time() - start < 15:               # Stop-хук рядом решает, ждать ли (не ждём, если он отдал ответ сам)
        l = read_json(path)
        if l and l.get("at", 0) >= start - 30 and not pid_alive(l.get("pid")):
            l["pid"] = os.getpid()
            write_json(path, l)
            me = l["gen"]
            break
        time.sleep(0.5)
    if not me:
        return 0
    log(f"listen {sid[:8]}")
    pump_fd = None
    next_poll = 0.0
    try:
        while time.time() - start < MAX_LISTEN:
            l = read_json(path)
            if not l or l.get("gen") != me:
                return 0
            ready = os.path.join(READY, sid)
            if os.path.exists(ready):
                _rm(ready)
                st, r = call("POST", "/cc/take", {"session_id": sid}, token=c["token"])
                if st == 401:
                    return 0
                msgs = r.get("messages") if st == 200 else None
                if msgs:
                    stop_listening(sid)
                    log(f"wake {sid[:8]}: {len(msgs)}")
                    got = fetch_files(c, sid, r.get("files"), 120)
                    sys.stderr.write(_phone_text(msgs, got))
                    sys.stderr.flush()
                    return 2
            if pump_fd is None:
                pump_fd = _try_pump()
            if pump_fd is not None and time.time() >= next_poll:
                next_poll = time.time() + _pump(c)
            time.sleep(1)
        return 0
    finally:
        if pump_fd is not None:
            os.close(pump_fd)


def _try_pump():
    """Один опрос сервера на Мак: кто взял замок, тот спрашивает за все ждущие сессии."""
    _mkdirs()
    fd = os.open(os.path.join(HOME, "pump.lock"), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except OSError:
        os.close(fd)
        return None


def _pump(c):
    """→ через сколько секунд спросить снова."""
    sids = []
    for f in os.listdir(LISTEN):
        if not f.endswith(".json"):
            continue
        l = read_json(os.path.join(LISTEN, f))
        sid = f[:-5]
        if l and pid_alive(l.get("pid")) and _sid_ok(sid):
            sids.append(sid)
        elif l and not pid_alive(l.get("pid")) and time.time() - l.get("at", 0) > 60:
            stop_listening(sid)                   # ожидатель умер (Claude закрыли) — не спрашивать за него
    if not sids:
        return 5
    st, r = call("POST", "/cc/poll", {"listening": sids, "away": away()}, token=c["token"])
    if st != 200:
        return 30 if st == 401 else 15
    for sid in r.get("ready") or []:
        if _sid_ok(sid):
            open(os.path.join(READY, sid), "w").close()
    return max(2, min(int(r.get("next") or 15), 60))


# ---------------------------------------------------------------- вход

HOOKS = {"start": hook_start, "prompt": hook_prompt, "stop": hook_stop, "notify": hook_notify, "end": hook_end,
         "permission": hook_permission, "listen": hook_listen, "pre": hook_pre, "post": hook_post}


def run_hook(name):
    """Хук никогда не ломает сессию: нет привязки, нет сети, ошибка — тихий выход 0."""
    try:
        inp = json.loads(sys.stdin.read() or "{}")
    except ValueError:
        return 0
    if os.environ.get("MEMORYAI_OFF") == "1" or not _sid_ok(inp.get("session_id")):
        return 0
    if name == "hello":                      # единственный хук, которому привязка не нужна
        try:
            return hook_hello(inp) or 0
        except Exception as e:  # noqa: BLE001
            log(f"hook hello: {type(e).__name__}: {e}")
            return 0
    c = conf()
    if not c or name not in HOOKS:
        return 0
    try:
        rc = HOOKS[name](c, inp)
        return rc if isinstance(rc, int) else 0
    except Exception as e:  # noqa: BLE001 — хук не должен ронять Claude Code
        log(f"hook {name}: {type(e).__name__}: {e}")
        return 0


def main(argv):
    if len(argv) >= 2 and argv[0] == "hook":
        return run_hook(argv[1])
    if argv[:2] == ["connect", "start"]:
        return connect_start(" ".join(argv[2:]))
    if argv[:2] == ["connect", "wait"]:
        return connect_wait()
    if argv[:1] == ["status"]:
        return status_cmd()
    if argv[:1] == ["disconnect"]:
        return disconnect_cmd()
    if len(argv) == 2 and argv[0] == "end-bg":
        end_bg(argv[1])
        return 0
    if len(argv) == 2 and argv[0] == "connect-bg":
        try:
            connect_bg(argv[1])
        except Exception as e:  # noqa: BLE001
            log(f"connect-bg: {type(e).__name__}: {e}")
        return 0
    if len(argv) == 2 and argv[0] == "perm-cancel":
        c = conf()                             # хук разрешения убит, а на Маке уже ответили
        if c and re.fullmatch(r"[A-Za-z0-9_-]{8,40}", argv[1]):
            call("POST", f"/cc/perm/{argv[1]}/cancel", {"why": "mac"}, token=c["token"])
        return 0
    if len(argv) == 2 and argv[0] == "sync-bg":
        try:
            sync_bg(argv[1])
        except Exception as e:  # noqa: BLE001
            log(f"sync-bg: {type(e).__name__}: {e}")
        return 0
    print(__doc__)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

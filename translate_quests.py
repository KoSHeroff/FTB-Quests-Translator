#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Перевод SNBT-квестов FTB Quests на русский через OpenAI API.

Установка (Windows / Linux / macOS):
  python3 -m venv .venv
  .venv\\Scripts\\activate   # Windows
  # source .venv/bin/activate  # Linux/macOS
  pip install -r requirements.txt
  copy .env.example .env       # OPENAI_API_KEY; при необходимости OPENAI_BASE_URL

Кэш переводов по умолчанию: translation_cache.json в папке со скриптом (не зависит от текущей директории в консоли).

Запуск:
  python translate_quests.py --input quests --output quests_ru
  python translate_quests.py --dry-run
  python translate_quests.py --verify-only --input quests --output quests_ru
  python translate_quests.py -v --input quests --output quests_ru   # подробный лог
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Iterator

import nbtlib
from dotenv import load_dotenv
from nbtlib import Compound, List, String
from openai import OpenAI

LOG = logging.getLogger("translate_quests")
# Каталог скрипта — для путей по умолчанию независимо от cwd
_SCRIPT_DIR = Path(__file__).resolve().parent

# --- FTB SNBT: между полями часто нет запятых; nbtlib требует запятые ---

_KEY_LINE = re.compile(r"^[a-zA-Z0-9_]+\s*:")
_INT_LINE = re.compile(r"^[\t\s]*-?\d+[bBsSlLfFdD]?\s*$")


def insert_snbt_commas(raw: str) -> str:
    """Вставляет недостающие запятые между элементами compound/list (экспорт FTB)."""
    lines = raw.split("\n")
    out: list[str] = []
    for i, line in enumerate(lines):
        out.append(line)
        if i + 1 >= len(lines):
            continue
        cur = line.rstrip()
        nxt = lines[i + 1].strip()
        if not cur or cur.endswith(","):
            continue
        if cur.endswith("{") or cur.endswith("["):
            continue
        if not nxt:
            continue
        if nxt.startswith("}") or nxt.startswith("]"):
            continue
        # Элементы списка строк подряд: "a"\n\t\t"b"
        nxt_ls = lines[i + 1].lstrip()
        if cur.rstrip().endswith('"') and nxt_ls.startswith('"'):
            out[-1] = cur.rstrip() + ","
            continue
        # Массивы целых [I; ... ] без запятых между числами
        if _INT_LINE.match(cur) and _INT_LINE.match(lines[i + 1]):
            out[-1] = cur.rstrip() + ","
            continue
        if nxt.startswith("{") or nxt.startswith("["):
            if cur.endswith("}") or cur.endswith("]"):
                out[-1] = cur + ","
            continue
        if _KEY_LINE.match(nxt):
            if ":" in cur:
                _, _, rest = cur.partition(":")
                rest = rest.strip()
                if rest.endswith("{") or rest.endswith("["):
                    continue
            out[-1] = cur + ","
    return "\n".join(out)


def parse_snbt_file(path: Path) -> Compound:
    text = path.read_text(encoding="utf-8")
    try:
        return nbtlib.parse_nbt(text)
    except nbtlib.literal.parser.InvalidLiteral:
        LOG.debug("SNBT без запятых, правка: %s", path)
        fixed = insert_snbt_commas(text)
        return nbtlib.parse_nbt(fixed)


def compound_to_snbt(root: Compound) -> str:
    return root.snbt(indent="\t", compact=False)


# --- JSON-компоненты в строках (subtitle с clickEvent) ---

def _walk_json_for_texts(obj: Any, acc: list[str]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "text" and isinstance(v, str):
                acc.append(v)
            else:
                _walk_json_for_texts(v, acc)
    elif isinstance(obj, list):
        for el in obj:
            _walk_json_for_texts(el, acc)


def _apply_json_for_texts(obj: Any, translations: Iterator[str]) -> Any:
    if isinstance(obj, dict):
        newd: dict[str, Any] = {}
        for k, v in obj.items():
            if k == "text" and isinstance(v, str):
                newd[k] = next(translations)
            else:
                newd[k] = _apply_json_for_texts(v, translations)
        return newd
    if isinstance(obj, list):
        return [_apply_json_for_texts(el, translations) for el in obj]
    return obj


def looks_like_json_text_component(s: str) -> bool:
    t = s.strip()
    return len(t) >= 2 and t[0] == "[" and t[-1] == "]"


def split_for_translation(s: str) -> tuple[str, list[str] | None]:
    """
    Возвращает ('plain', None) или ('json', list of text segments для перевода).
    """
    if not s.strip():
        return ("empty", None)
    if s.strip() == "{@pagebreak}":
        return ("literal", None)
    if looks_like_json_text_component(s):
        try:
            data = json.loads(s)
        except json.JSONDecodeError:
            return ("plain", None)
        texts: list[str] = []
        _walk_json_for_texts(data, texts)
        if not texts:
            return ("plain", None)
        return ("json", texts)
    return ("plain", None)


def merge_json_translation(original: str, translated_segments: list[str]) -> str:
    data = json.loads(original)
    it = iter(translated_segments)
    data = _apply_json_for_texts(data, it)
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


# --- Обход NBT: только whitelist ключей ---

TRANSLATABLE_KEYS = frozenset({"title", "subtitle", "lock_message"})
LIST_TEXT_KEYS = frozenset({"description", "hover"})

Slot = tuple[Compound | List, str | int]


def _hash_key(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def iter_translatable_slots(obj: Any) -> Iterator[Slot]:
    """String в nbtlib — подкласс str; подмена через container[key] = String(...)."""
    if isinstance(obj, Compound):
        for key in obj:
            val = obj[key]
            if key in TRANSLATABLE_KEYS and isinstance(val, String):
                yield obj, key
            elif key in LIST_TEXT_KEYS and isinstance(val, List):
                for i, elem in enumerate(val):
                    if isinstance(elem, String):
                        yield val, i
            else:
                yield from iter_translatable_slots(val)
    elif isinstance(obj, List):
        for elem in obj:
            yield from iter_translatable_slots(elem)


def apply_translations_from_cache(root: Compound, cache: dict[str, str]) -> None:
    """Применяет переводы из кэша по хэшу исходной строки."""

    def apply_one(container: Compound | List, key: str | int, original: str) -> None:
        kind, extra = split_for_translation(original)
        if kind == "empty" or kind == "literal":
            return
        if kind == "json" and extra:
            joined = "\n---SEG---\n".join(extra)
            tr = cache.get(_hash_key(joined))
            if tr is None:
                return
            parts = tr.split("\n---SEG---\n")
            if len(parts) < len(extra):
                parts = (parts + [""] * len(extra))[: len(extra)]
            container[key] = String(merge_json_translation(original, parts[: len(extra)]))
            return
        tr = cache.get(_hash_key(original))
        if tr is not None:
            container[key] = String(tr)

    for container, slot in iter_translatable_slots(root):
        cur = container[slot]
        assert isinstance(cur, String)
        apply_one(container, slot, str(cur))


def collect_all_original_strings(root: Compound) -> list[str]:
    originals: list[str] = []
    seen: set[str] = set()

    def add(s: str) -> None:
        kind, extra = split_for_translation(s)
        if kind == "empty" or kind == "literal":
            return
        if kind == "json" and extra:
            joined = "\n---SEG---\n".join(extra)
            h = _hash_key(joined)
            if h not in seen:
                seen.add(h)
                originals.append(joined)
            return
        h = _hash_key(s)
        if h not in seen:
            seen.add(h)
            originals.append(s)

    for container, slot in iter_translatable_slots(root):
        cur = container[slot]
        assert isinstance(cur, String)
        add(str(cur))
    return originals


def load_cache(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_cache(path: Path, cache: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


SYSTEM_PROMPT = """Ты переводишь строки из книги квестов Minecraft (мод FTB Quests) на русский язык.
Правила:
- Сохраняй все коды форматирования Minecraft: символ & с последующим символом (например &a, &l, &r, &6).
- Сохраняй переносы строк и экранирование: \\n, \\\\n, \\\\&.
- Не переводи и не изменяй строку {@pagebreak} если она есть отдельной строкой.
- Не переводи имена собственные модов и идентификаторы предметов (minecraft:..., modid:...).
- Если вход разбит маркером строк ---SEG---, это части одного JSON; переведи каждую часть отдельно, сохрани разделители ---SEG--- в ответе между переведёнными частями в том же количестве.
Ответ: только переведённые блоки в формате ниже (сохрани номера [001], [002], ...)."""


def translate_batch(
    client: OpenAI,
    model: str,
    batch: list[str],
) -> list[str]:
    if not batch:
        return []
    blocks = [f"[{i:03d}]\n{b}" for i, b in enumerate(batch, start=1)]
    user_content = (
        "Переведи каждый блок на русский. Сохрани строки вида [001], [002], ... "
        "перед каждым блоком в том же порядке.\n\n" + "\n\n".join(blocks)
    )
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        temperature=0.2,
    )
    text = (resp.choices[0].message.content or "").strip()
    found: dict[int, str] = {}
    for m in re.finditer(r"\[(\d{3})\]\s*\n", text):
        idx = int(m.group(1))
        start = m.end()
        nxt = re.search(r"\n\[\d{3}\]\s*\n", text[start:])
        end = start + nxt.start() if nxt else len(text)
        found[idx] = text[start:end].strip()
    out = [found.get(i, batch[i - 1]) for i in range(1, len(batch) + 1)]
    if len(batch) == 1 and not found:
        return [text]
    return out


def translate_all_strings(
    client: OpenAI | None,
    model: str,
    strings: list[str],
    cache: dict[str, str],
    batch_size: int,
    dry_run: bool,
    cache_path: Path | None = None,
) -> dict[str, str]:
    """Возвращает обновлённый кэш hash -> перевод (sha256 исходной строки -> перевод)."""
    pending: list[str] = []
    pending_keys: list[str] = []

    for s in strings:
        hk = _hash_key(s)
        if hk in cache:
            continue
        pending.append(s)
        pending_keys.append(hk)

    n_cached = len(strings) - len(pending)
    if pending:
        LOG.info(
            "  строк: всего уникальных в файле %d, уже в кэше %d, к переводу %d",
            len(strings),
            n_cached,
            len(pending),
        )
    else:
        LOG.info(
            "  строк: %d — все уже в кэше, API не нужен",
            len(strings),
        )

    if dry_run or not pending:
        return cache

    if client is None:
        raise RuntimeError("client обязателен для вызова API")

    n_batches = (len(pending) + batch_size - 1) // batch_size
    for bi, i in enumerate(range(0, len(pending), batch_size), start=1):
        chunk = pending[i : i + batch_size]
        keys_chunk = pending_keys[i : i + batch_size]
        LOG.info(
            "  OpenAI: пакет %d/%d (%d строк), модель %s",
            bi,
            n_batches,
            len(chunk),
            model,
        )
        for attempt in range(4):
            try:
                out = translate_batch(client, model, chunk)
                break
            except Exception as e:
                wait = 2**attempt
                LOG.warning("  API ошибка (%s), повтор через %ds...", e, wait)
                time.sleep(wait)
        else:
            raise RuntimeError("OpenAI API недоступен после нескольких попыток")

        if len(out) != len(chunk):
            LOG.warning(
                "  ответ длины %d вместо %d — добор по одной строке",
                len(out),
                len(chunk),
            )
            if len(chunk) == 1:
                cache[keys_chunk[0]] = out[0]
            else:
                for k, s in zip(keys_chunk, chunk):
                    LOG.debug("  одиночный запрос для хэша %s...", k[:12])
                    single = translate_batch(client, model, [s])
                    cache[k] = single[0]
        else:
            for k, t in zip(keys_chunk, out):
                cache[k] = t

        if cache_path is not None:
            save_cache(cache_path, cache)
            LOG.info("  кэш на диске: %s (%d записей)", cache_path, len(cache))

    return cache


def gather_snbt_files(base: Path, skip_reward_tables: bool) -> list[Path]:
    files: list[Path] = []
    for p in sorted(base.rglob("*.snbt")):
        if skip_reward_tables and "reward_tables" in p.parts:
            continue
        files.append(p)
    return files


def extract_ids_from_snbt_text(text: str) -> list[str]:
    return re.findall(r'\bid:\s*"([0-9A-Fa-f]+)"', text)


def verify_trees(inp: Path, out: Path) -> bool:
    ok = True
    for in_file in gather_snbt_files(inp, skip_reward_tables=False):
        rel = in_file.relative_to(inp)
        out_file = out / rel
        if not out_file.is_file():
            LOG.error("verify: отсутствует выходной файл: %s", out_file)
            ok = False
            continue
        if "reward_tables" in in_file.parts:
            continue
        a = extract_ids_from_snbt_text(in_file.read_text(encoding="utf-8"))
        b = extract_ids_from_snbt_text(out_file.read_text(encoding="utf-8"))
        if a != b:
            LOG.error("verify: несовпадение id в %s: %d vs %d", rel, len(a), len(b))
            ok = False
    return ok


def _setup_logging(verbose: bool) -> None:
    try:
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8")
    except OSError:
        pass
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
        force=True,
    )


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Перевод FTB Quests SNBT на русский (OpenAI).")
    parser.add_argument("--input", type=Path, default=Path("quests"), help="Входная папка квестов")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("quests_ru"),
        help="Выходная папка (по умолчанию quests_ru)",
    )
    parser.add_argument(
        "--cache",
        type=Path,
        default=_SCRIPT_DIR / "translation_cache.json",
        help="Файл кэша переводов (по умолчанию рядом со скриптом, не от cwd)",
    )
    parser.add_argument("--batch-size", type=int, default=40)
    parser.add_argument("--dry-run", action="store_true", help="Только собрать строки, без API")
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Сравнить id: в input и output, без перевода",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Подробный лог (DEBUG): SNBT, одиночные запросы API",
    )
    args = parser.parse_args()
    _setup_logging(args.verbose)

    inp = args.input.resolve()
    out = args.output.resolve()

    if args.verify_only:
        LOG.info("Режим проверки: сравнение id в %s и %s", inp, out)
        ok = verify_trees(inp, out)
        LOG.info("Проверка %s", "пройдена" if ok else "с ошибками")
        sys.exit(0 if ok else 1)

    cache_path = args.cache.resolve()
    cache = load_cache(cache_path)
    LOG.info(
        "Старт: вход=%s, выход=%s, кэш=%s, batch=%d",
        inp,
        out,
        cache_path,
        args.batch_size,
    )
    if not cache:
        LOG.info("Кэш пустой или отсутствует — строки будут переведены через API (кроме --dry-run).")
    else:
        LOG.info("Загружен кэш: %d записей", len(cache))

    files = gather_snbt_files(inp, skip_reward_tables=False)
    snbt_files = [f for f in files if "reward_tables" not in f.parts]
    reward_files = [f for f in files if "reward_tables" in f.parts]

    client: OpenAI | None = None
    model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    if not args.dry_run:
        key = os.environ.get("OPENAI_API_KEY")
        if not key:
            LOG.error("Задайте OPENAI_API_KEY в .env или окружении (или --dry-run).")
            sys.exit(1)
        client_kw: dict[str, str] = {"api_key": key}
        base_url = (os.environ.get("OPENAI_BASE_URL") or "").strip()
        if base_url:
            client_kw["base_url"] = base_url
            LOG.info("OpenAI base URL: %s", base_url)
        client = OpenAI(**client_kw)
        LOG.info("Модель OpenAI: %s", model)
    else:
        LOG.warning(
            "Режим --dry-run: API не вызывается, применяется только кэш %s",
            cache_path,
        )

    total_strings = 0
    total_new = 0
    n_snbt = len(snbt_files)
    idx = 0

    try:
        for in_file in files:
            rel = in_file.relative_to(inp)
            out_file = out / rel
            if "reward_tables" in in_file.parts:
                out_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(in_file, out_file)
                LOG.info("Копия (без перевода): %s", rel)
                continue

            idx += 1
            LOG.info("[%d/%d] %s", idx, n_snbt, rel)

            root = parse_snbt_file(in_file)
            strings = collect_all_original_strings(root)
            total_strings += len(strings)
            n_new = sum(1 for s in strings if _hash_key(s) not in cache)
            total_new += n_new

            if args.dry_run:
                translate_all_strings(
                    None,
                    model,
                    strings,
                    cache,
                    args.batch_size,
                    dry_run=True,
                    cache_path=None,
                )
            else:
                translate_all_strings(
                    client,
                    model,
                    strings,
                    cache,
                    args.batch_size,
                    dry_run=False,
                    cache_path=cache_path,
                )

            apply_translations_from_cache(root, cache)

            out_file.parent.mkdir(parents=True, exist_ok=True)
            out_file.write_text(compound_to_snbt(root) + "\n", encoding="utf-8")
            LOG.info("  записано: %s", out_file)

        LOG.info("—")
        LOG.info("Готово. SNBT-файлов обработано: %d", len(snbt_files))
        LOG.info("Файлов reward_tables скопировано: %d", len(reward_files))
        LOG.info(
            "Уникальных строк по файлам (сумма; глобальная дедупликация в кэше): %d",
            total_strings,
        )
        LOG.info("Новых строк относительно кэша на старте (оценка): %d", total_new)
        if args.dry_run:
            LOG.warning(
                "Режим --dry-run: новые строки не переводились; кэш не пополнялся через API.",
            )
    except KeyboardInterrupt:
        LOG.warning("Остановка по Ctrl+C — кэш будет сохранён.")
        raise
    finally:
        save_cache(cache_path, cache)
        LOG.info("Кэш сохранён: %s (%d записей)", cache_path, len(cache))


if __name__ == "__main__":
    main()

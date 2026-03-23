import asyncio
import threading
import logging
from flask import Flask, request, jsonify

from config import DB_PATH
from nemesis_service import NemesisService
from updater_service import WCAUpdater

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

_nemesis_service: NemesisService | None = None
_update_lock = threading.Lock()
_update_running = False
_periodic_thread: threading.Thread | None = None
_periodic_stop = threading.Event()
MAX_NEMESIS_LIST_SIZE = 10


def get_nemesis_service() -> NemesisService:
    global _nemesis_service
    if _nemesis_service is None:
        _nemesis_service = NemesisService(DB_PATH)
    return _nemesis_service


async def _run_update(force: bool) -> bool:
    updater = WCAUpdater(DB_PATH)
    try:
        ok = await updater.update_database(force=force)
        return ok
    finally:
        await updater.close()


def trigger_update(force: bool):
    global _update_running, _nemesis_service

    def runner():
        global _update_running, _nemesis_service
        try:
            logger.info(f"开始{'强制' if force else '检查'}更新数据库...")
            ok = asyncio.run(_run_update(force))
            if ok:
                _nemesis_service = None  # 重新加载使用新 DB
                logger.info("数据库更新检查完成")
            else:
                logger.warning("数据库更新检查失败")
        except Exception as e:
            logger.exception("更新数据库失败: %s", e)
        finally:
            _update_running = False

    with _update_lock:
        if _update_running:
            return False
        _update_running = True
        thread = threading.Thread(target=runner, daemon=True)
        thread.start()
        return True


def _periodic_update_loop(interval_hours: int = 3):
    """后台定时检查更新。"""
    # 首次启动时，根据是否已有 DB 决定是否 force
    logger.info("启动定时更新任务，每 {} 小时检查一次".format(interval_hours))
    if DB_PATH.exists():
        logger.info("检测到现有数据库，将进行智能更新检查")
        trigger_update(force=False)
    else:
        logger.info("未检测到数据库，将进行初始化下载")
        trigger_update(force=True)

    while not _periodic_stop.wait(interval_hours * 3600):
        logger.info("定时检查：开始检查数据库更新...")
        trigger_update(force=False)


def start_periodic_updates():
    global _periodic_thread
    if _periodic_thread and _periodic_thread.is_alive():
        return
    _periodic_thread = threading.Thread(target=_periodic_update_loop, daemon=True)
    _periodic_thread.start()


def _maybe_return_nemesis_list(count: int, people: list[dict]) -> list[dict]:
    """人数过多时不返回明细列表，避免响应体过大。"""
    if count <= MAX_NEMESIS_LIST_SIZE:
        return people
    return []


@app.route("/nemesis", methods=["POST"])
def nemesis_api():
    data = request.get_json(force=True, silent=True) or {}
    person_id = (data.get("person_id") or "").strip()
    if not person_id:
        return jsonify({"error": "person_id is required"}), 400
    if not DB_PATH.exists():
        return jsonify({"error": f"database not found at {DB_PATH}"}), 500
    try:
        svc = get_nemesis_service()
        result = svc.query(person_id)
        continent, world_count, continent_count, country_count, world_list, continent_list, country_list = result
        return jsonify(
            {
                "continent": continent,
                "world_count": world_count,
                "continent_count": continent_count,
                "country_count": country_count,
                "world_list": _maybe_return_nemesis_list(world_count, world_list),
                "continent_list": _maybe_return_nemesis_list(continent_count, continent_list),
                "country_list": _maybe_return_nemesis_list(country_count, country_list),
            }
        )
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        logger.exception("宿敌查询失败: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/version", methods=["GET"])
def db_version():
    if not DB_PATH.exists():
        return jsonify({"error": f"database not found at {DB_PATH}"}), 500

    updater = WCAUpdater(DB_PATH)
    export_date = updater.get_database_version()
    if not export_date:
        return jsonify({"error": "database export_date not found"}), 500

    return jsonify({"export_date": export_date})


if __name__ == "__main__":
    # 启动时触发一次初始化，并开启后台定时检查
    logger.info("=" * 60)
    logger.info("WCA 服务启动中...")
    logger.info(f"数据库路径: {DB_PATH}")
    logger.info("=" * 60)
    start_periodic_updates()
    logger.info("Flask 服务启动在 0.0.0.0:8000")
    app.run(host="0.0.0.0", port=8000)


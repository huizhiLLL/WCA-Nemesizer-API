import asyncio
import threading
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
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
_update_last_result: dict | None = None
_periodic_thread: threading.Thread | None = None
_periodic_stop = threading.Event()
_update_interval_hours = 6
_last_check_time: float | None = None
_next_check_time: float | None = None


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
    global _update_running, _update_last_result, _nemesis_service, _last_check_time

    def runner():
        global _update_running, _update_last_result, _nemesis_service, _last_check_time
        try:
            logger.info(f"开始{'强制' if force else '检查'}更新数据库...")
            ok = asyncio.run(_run_update(force))
            _update_last_result = {
                "success": ok,
                "timestamp": time.time(),
                "datetime": datetime.now().isoformat(),
                "force": force
            }
            _last_check_time = time.time()
            if ok:
                _nemesis_service = None  # 重新加载使用新 DB
                logger.info("数据库更新检查完成")
            else:
                logger.warning("数据库更新检查失败")
        except Exception as e:
            logger.exception("更新数据库失败: %s", e)
            _update_last_result = {
                "success": False,
                "error": str(e),
                "timestamp": time.time(),
                "datetime": datetime.now().isoformat()
            }
            _last_check_time = time.time()
        finally:
            _update_running = False

    with _update_lock:
        if _update_running:
            return False
        _update_running = True
        thread = threading.Thread(target=runner, daemon=True)
        thread.start()
        return True


def _periodic_update_loop(interval_hours: int = 6):
    """后台定时检查更新（默认每 6 小时）"""
    global _periodic_stop, _update_interval_hours, _next_check_time
    _update_interval_hours = interval_hours
    
    # 首次启动时，根据是否已有 DB 决定是否 force
    logger.info("启动定时更新任务，每 {} 小时检查一次".format(interval_hours))
    if DB_PATH.exists():
        logger.info("检测到现有数据库，将进行智能更新检查")
        trigger_update(force=False)
    else:
        logger.info("未检测到数据库，将进行初始化下载")
        trigger_update(force=True)
    
    # 计算下次检查时间
    _next_check_time = time.time() + interval_hours * 3600
    
    while not _periodic_stop.wait(interval_hours * 3600):
        logger.info("定时检查：开始检查数据库更新...")
        trigger_update(force=False)
        # 更新下次检查时间
        _next_check_time = time.time() + interval_hours * 3600
        next_check_datetime = datetime.fromtimestamp(_next_check_time)
        logger.info(f"下次检查时间: {next_check_datetime.strftime('%Y-%m-%d %H:%M:%S')}")


def start_periodic_updates():
    global _periodic_thread
    if _periodic_thread and _periodic_thread.is_alive():
        return
    _periodic_thread = threading.Thread(target=_periodic_update_loop, daemon=True)
    _periodic_thread.start()


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
                "world_list": world_list,
                "continent_list": continent_list,
                "country_list": country_list,
            }
        )
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        logger.exception("宿敌查询失败: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/db/info", methods=["GET"])
def db_info():
    updater = WCAUpdater(DB_PATH)
    info = updater.get_database_info()
    valid = updater.verify_database()
    
    # 计算下次检查时间
    next_check_info = None
    if _next_check_time:
        next_check_info = {
            "timestamp": _next_check_time,
            "datetime": datetime.fromtimestamp(_next_check_time).isoformat(),
            "remaining_seconds": max(0, int(_next_check_time - time.time())),
            "remaining_hours": round(max(0, (_next_check_time - time.time()) / 3600), 2)
        }
    
    return jsonify(
        {
            "db_path": str(DB_PATH),
            "exists": DB_PATH.exists(),
            "valid": valid,
            "info": info,
            "update_running": _update_running,
            "last_update": _update_last_result,
            "periodic_update": {
                "enabled": _periodic_thread is not None and _periodic_thread.is_alive(),
                "interval_hours": _update_interval_hours,
                "last_check_time": _last_check_time,
                "last_check_datetime": datetime.fromtimestamp(_last_check_time).isoformat() if _last_check_time else None,
                "next_check": next_check_info
            }
        }
    )


@app.route("/db/update", methods=["POST"])
def db_update():
    force = bool((request.get_json(silent=True) or {}).get("force"))
    with _update_lock:
        if _update_running:
            return jsonify({"error": "update already running"}), 409
        trigger_update(force)
        return jsonify({"message": "update started", "force": force})


if __name__ == "__main__":
    # 启动时触发一次初始化，并开启每 6 小时的定时检查
    logger.info("=" * 60)
    logger.info("WCA 服务启动中...")
    logger.info(f"数据库路径: {DB_PATH}")
    logger.info("=" * 60)
    start_periodic_updates()
    logger.info("Flask 服务启动在 0.0.0.0:8000")
    app.run(host="0.0.0.0", port=8000)


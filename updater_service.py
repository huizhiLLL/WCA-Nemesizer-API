import asyncio
import json
import sqlite3
import zipfile
import tempfile
from pathlib import Path
from typing import Optional
import aiohttp
import pandas as pd
import logging

from config import WCA_EXPORT_API, REQUEST_TIMEOUT, CHUNKSIZE, DB_PATH

logger = logging.getLogger(__name__)


class WCAUpdater:
    """WCA 数据库更新器（独立后端版本）"""

    def __init__(self, db_path: str | Path = DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def get_export_info(self) -> Optional[dict]:
        session = await self._ensure_session()
        try:
            async with session.get(WCA_EXPORT_API) as response:
                if response.status == 200:
                    return await response.json()
                logger.error(f"获取 WCA 导出信息失败，状态码：{response.status}")
                return None
        except asyncio.TimeoutError:
            logger.error(f"获取 WCA 导出信息超时（{REQUEST_TIMEOUT}秒）")
            return None
        except Exception as e:
            logger.error(f"获取 WCA 导出信息异常: {e}")
            return None

    async def download_tsv_archive(self, tsv_url: str) -> Optional[Path]:
        session = await self._ensure_session()
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
        temp_path = Path(temp_file.name)
        temp_file.close()
        try:
            logger.info(f"开始下载 WCA TSV 压缩包: {tsv_url}")
            async with session.get(tsv_url) as response:
                if response.status != 200:
                    logger.error(f"下载 WCA TSV 压缩包失败，状态码：{response.status}")
                    temp_path.unlink(missing_ok=True)
                    return None
                with open(temp_path, "wb") as f:
                    async for chunk in response.content.iter_chunked(8192):
                        f.write(chunk)
            logger.info(f"WCA TSV 压缩包下载完成: {temp_path}")
            return temp_path
        except asyncio.TimeoutError:
            logger.error(f"下载 WCA TSV 压缩包超时（{REQUEST_TIMEOUT}秒）")
            temp_path.unlink(missing_ok=True)
            return None
        except Exception as e:
            logger.error(f"下载 WCA TSV 压缩包异常: {e}")
            temp_path.unlink(missing_ok=True)
            return None

    def _find_tsv_file(self, temp_dir_path: Path, table: str) -> Path | None:
        patterns = [
            f"{table}.tsv",
            f"WCA_export_{table}.tsv",
            f"{table}.TSV",
            f"WCA_export_{table}.TSV",
        ]
        for pattern in patterns:
            for path in temp_dir_path.rglob(pattern):
                if path.is_file():
                    return path
        return None

    def _process_single_table(self, conn: sqlite3.Connection, table_name: str, tsv_file: Path) -> bool:
        try:
            logger.info(f"正在处理表: {table_name}")
            chunk_count = 0
            total_rows = 0
            is_first_chunk = True

            for chunk in pd.read_csv(
                tsv_file,
                sep="\t",
                chunksize=CHUNKSIZE,
                encoding="utf-8",
                low_memory=False,
            ):
                chunk_count += 1
                total_rows += len(chunk)
                if is_first_chunk:
                    chunk.to_sql(table_name, conn, if_exists="replace", index=False)
                    is_first_chunk = False
                else:
                    chunk.to_sql(table_name, conn, if_exists="append", index=False)
                if chunk_count % 10 == 0:
                    conn.commit()
                    logger.info(f"表 {table_name}: 已处理 {chunk_count} 块，累计 {total_rows} 行")
            conn.commit()
            logger.info(f"表 {table_name} 处理完成: {total_rows} 行")
            return True
        except Exception as e:
            logger.error(f"处理表 {table_name} 时出错: {e}")
            return False

    def _create_database_indexes(self, conn: sqlite3.Connection) -> None:
        logger.info("正在创建索引...")
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_persons_wca_id ON persons(wca_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_rankssingle_person_id ON ranks_single(person_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_rankssingle_event_id ON ranks_single(event_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ranksaverage_person_id ON ranks_average(person_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ranksaverage_event_id ON ranks_average(event_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_id ON events(id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_competitions_id ON competitions(id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_competitions_country_id ON competitions(country_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_competitions_start_date ON competitions(start_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_competitions_end_date ON competitions(end_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_rankssingle_event_best ON ranks_single(event_id, best)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ranksaverage_event_best ON ranks_average(event_id, best)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_rankssingle_event_best_person ON ranks_single(event_id, best, person_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ranksaverage_event_best_person ON ranks_average(event_id, best, person_id)")
            conn.commit()
            logger.info("索引创建完成")
        except Exception as e:
            logger.warning(f"创建索引时出错（可忽略）: {e}")

    def process_tsv_to_sqlite(self, tsv_archive_path: Path, export_date: str | None = None) -> bool:
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_dir_path = Path(temp_dir)
                logger.info("正在解压 TSV 压缩包...")
                with zipfile.ZipFile(tsv_archive_path, "r") as zip_ref:
                    zip_ref.extractall(temp_dir_path)

                logger.info("开始处理 TSV 文件并转换为 SQLite 数据库...")
                conn = sqlite3.connect(str(self.db_path))

                required_tables = [
                    "countries",
                    "events",
                    "persons",
                    "competitions",
                    "ranks_single",
                    "ranks_average",
                ]

                for table_name in required_tables:
                    tsv_file = self._find_tsv_file(temp_dir_path, table_name)
                    if not tsv_file:
                        logger.warning(f"TSV 文件不存在: {table_name}.tsv")
                        continue
                    if not self._process_single_table(conn, table_name, tsv_file):
                        conn.close()
                        return False

                self._create_database_indexes(conn)
                
                # 保存导出日期到 metadata 表
                if export_date:
                    try:
                        conn.execute("""
                            CREATE TABLE IF NOT EXISTS metadata (
                                export_date TEXT,
                                export_version TEXT,
                                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            )
                        """)
                        conn.execute("DELETE FROM metadata")
                        conn.execute(
                            "INSERT INTO metadata (export_date) VALUES (?)",
                            (export_date,)
                        )
                        conn.commit()
                        logger.info(f"已保存导出日期: {export_date}")
                    except Exception as e:
                        logger.warning(f"保存导出日期失败（可忽略）: {e}")
                
                conn.close()
                logger.info("TSV 文件处理完成，SQLite 数据库已创建")
                return True
        except Exception as e:
            logger.error(f"处理 TSV 文件时出错: {e}")
            return False

    async def update_database(self, force: bool = False) -> bool:
        # 如果不是强制更新，先检查是否需要更新
        if self.db_path.exists() and not force:
            if self.verify_database():
                # 检查 API 是否有新版本
                export_info = await self.get_export_info()
                if export_info:
                    api_export_date = export_info.get("export_date")
                    local_export_date = self._get_local_export_date()
                    
                    if api_export_date and local_export_date:
                        if api_export_date == local_export_date:
                            logger.info(f"WCA 数据库已是最新版本: {self.db_path} (export_date: {local_export_date})")
                            return True
                        else:
                            logger.info(f"检测到新版本: 本地={local_export_date}, API={api_export_date}")
                    else:
                        # 如果无法比较日期，使用文件修改时间作为备选
                        logger.info(f"WCA 数据库已存在: {self.db_path}")
                        return True
                else:
                    # API 调用失败，如果数据库有效则跳过更新
                    logger.warning("无法获取 WCA 导出信息，使用现有数据库")
                    return True
            else:
                logger.info("现有 WCA 数据库缺少必要表，准备重新下载并构建")

        export_info = await self.get_export_info()
        if not export_info:
            logger.error("无法获取 WCA 导出信息")
            return False

        tsv_url = export_info.get("tsv_url")
        if not tsv_url:
            logger.error("WCA 导出信息中未找到 TSV 压缩包 URL")
            return False

        tsv_archive_path = await self.download_tsv_archive(tsv_url)
        if not tsv_archive_path:
            logger.error("下载 TSV 压缩包失败")
            return False

        try:
            success = await asyncio.to_thread(self.process_tsv_to_sqlite, tsv_archive_path, export_info.get("export_date"))
            if success:
                if self.verify_database():
                    logger.info("WCA 数据库更新成功并验证通过")
                    return True
                logger.error("WCA 数据库文件验证失败")
                return False
            return False
        finally:
            try:
                if tsv_archive_path.exists():
                    tsv_archive_path.unlink()
                    logger.info("已清理临时 TSV 压缩包文件")
            except Exception as e:
                logger.warning(f"清理临时文件时出错: {e}")

    def verify_database(self) -> bool:
        if not self.db_path.exists():
            return False
        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            required_tables = ["persons", "events", "competitions", "ranks_single", "ranks_average", "countries"]
            placeholders = ",".join(["?"] * len(required_tables))
            cursor.execute(
                f"""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name IN ({placeholders})
                """,
                required_tables,
            )
            existing_tables = [row[0] for row in cursor.fetchall()]
            conn.close()
            if len(existing_tables) == len(required_tables):
                logger.info("WCA 数据库验证通过")
                return True
            missing = set(required_tables) - set(existing_tables)
            logger.error(f"WCA 数据库缺少必要的表: {missing}")
            return False
        except Exception as e:
            logger.error(f"验证 WCA 数据库时出错: {e}")
            return False

    def _get_local_export_date(self) -> str | None:
        """获取本地数据库的导出日期"""
        if not self.db_path.exists():
            return None
        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT export_date FROM metadata LIMIT 1")
                row = cursor.fetchone()
                if row and row[0]:
                    return row[0]
            except sqlite3.OperationalError:
                pass
            conn.close()
            return None
        except Exception as e:
            logger.debug(f"获取本地导出日期失败: {e}")
            return None

    def get_database_info(self) -> Optional[dict]:
        if not self.db_path.exists():
            return None
        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            info = None
            try:
                cursor.execute("SELECT * FROM metadata")
                metadata_row = cursor.fetchone()
                if metadata_row:
                    info = dict(zip([d[0] for d in cursor.description], metadata_row))
            except sqlite3.OperationalError:
                pass
            mtime = self.db_path.stat().st_mtime
            export_date = self._get_local_export_date()
            conn.close()
            result = info or {"file_mtime": mtime, "file_path": str(self.db_path)}
            if export_date:
                result["export_date"] = export_date
            return result
        except Exception as e:
            logger.error(f"获取数据库信息时出错: {e}")
            return None


from pathlib import Path
import os

# 数据库路径，默认相对路径，支持通过环境变量覆盖
DB_PATH = Path(os.environ.get("WCA_DB_PATH", "data/wca_data.db"))

# 下载与处理配置
WCA_EXPORT_API = "https://www.worldcubeassociation.org/api/v0/export/public"
REQUEST_TIMEOUT = 600  # 秒
CHUNKSIZE = 10000


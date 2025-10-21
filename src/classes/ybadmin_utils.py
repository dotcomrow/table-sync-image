import re
import os
import subprocess

from classes.config_reader import ConfigKeys, YugabyteDBKeys
from classes.logging import Logging
from classes.table_info import TableInfo

class YBAdminUtils:
    def __init__(self, config, logger: Logging):
        self.config = config
        self.logger = logger
    
    def delete_stream(self, stream_id: str):
        """Delete a CDC stream using yb-admin."""
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Deleting CDC stream", stream_id=stream_id)

        master_addrs = (
            self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.MASTER_ADDRESSES.value)
            or os.getenv("YB_MASTER_ADDRESSES")
        )
        if not master_addrs:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Master addresses not configured")
            raise ValueError("Master addresses not configured")

        yb_admin_bin = self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.YB_ADMIN_PATH.value, "yb-admin")
        self.logger.logMessage(Logging.LogLevel.DEBUG, "yb-admin binary resolved", yb_admin_bin=yb_admin_bin)

        try:
            out = subprocess.check_output(
                [yb_admin_bin, "--master_addresses", master_addrs, "delete_change_data_stream", stream_id],
                text=True, stderr=subprocess.STDOUT, timeout=20
            )
            self.logger.logMessage(Logging.LogLevel.DEBUG, "yb-admin delete_change_data_stream output", output=out)
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Deleted CDC stream ID", stream_id=stream_id)
        except subprocess.CalledProcessError as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to delete CDC stream", error=str(e))
            raise RuntimeError(f"Failed to delete CDC stream: {e}")

    def create_stream(self, database_name: str) -> str:
        """Create a CDC stream for a given database using yb-admin."""
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Creating CDC stream", database_name=database_name)

        master_addrs = (
            self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.MASTER_ADDRESSES.value)
            or os.getenv("YB_MASTER_ADDRESSES")
        )
        if not master_addrs:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Master addresses not configured")
            raise ValueError("Master addresses not configured")

        yb_admin_bin = self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.YB_ADMIN_PATH.value, "yb-admin")
        namespace = f"ysql.{database_name}"
        self.logger.logMessage(Logging.LogLevel.DEBUG, "yb-admin binary and namespace resolved", yb_admin_bin=yb_admin_bin, namespace=namespace)

        try:
            out = subprocess.check_output(
                [yb_admin_bin, "--master_addresses", master_addrs, "create_change_data_stream", namespace],
                text=True, stderr=subprocess.STDOUT, timeout=20
            )
            self.logger.logMessage(Logging.LogLevel.DEBUG, "yb-admin create_change_data_stream output", output=out)
            match = re.search(r"CDC Stream ID:\s*([0-9a-f]{32})", out, re.I)
            if match:
                stream_id = match.group(1)
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Created CDC stream ID", stream_id=stream_id)
                return stream_id
        except subprocess.CalledProcessError as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to create CDC stream", error=str(e))
            raise RuntimeError(f"Failed to create CDC stream: {e}")

        self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to create CDC stream: No stream ID found")
        raise RuntimeError("Failed to create CDC stream: No stream ID found")
    
    def verify_table_covered_by_stream(self, stream_id: str, table_info: TableInfo) -> bool:
        """Verify if a table is covered by a given CDC stream using yb-admin."""
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Verifying table coverage by CDC stream", stream_id=stream_id, table=table_info.to_dict())

        master_addrs = (
            self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.MASTER_ADDRESSES.value)
            or os.getenv("YB_MASTER_ADDRESSES")
        )
        if not master_addrs:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Master addresses not configured")
            raise ValueError("Master addresses not configured")

        yb_admin_bin = self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.YB_ADMIN_PATH.value, "yb-admin")
        self.logger.logMessage(Logging.LogLevel.DEBUG, "yb-admin binary resolved", yb_admin_bin=yb_admin_bin)

        try:
            table_id = self.get_table_id(yb_admin_bin, table_info)
            out = subprocess.check_output(
                [yb_admin_bin, "--master_addresses", master_addrs, "list_change_data_stream_tables", stream_id],
                text=True, stderr=subprocess.STDOUT, timeout=20
            )
            self.logger.logMessage(Logging.LogLevel.DEBUG, "yb-admin list_change_data_stream_tables output", output=out)
            for line in out.splitlines():
                if table_id in line:
                    self.logger.logMessage(Logging.LogLevel.DEBUG, "Table is covered by CDC stream", stream_id=stream_id, table=table_info.to_dict())
                    return True
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Table is NOT covered by CDC stream", stream_id=stream_id, table=table_info.to_dict())
            return False
        except subprocess.CalledProcessError as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to verify table coverage by CDC stream", error=str(e))
            raise RuntimeError(f"Failed to verify table coverage by CDC stream: {e}")
        
    def get_table_id(self, yb_admin_bin: str, table_info: TableInfo) -> str:
        """
        Expects table_info.database, table_info.schema, table_info.table
        Returns the Yugabyte table UUID for ysql.<db>.<schema>.<table>.
        """
        master_addrs = (
            self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.MASTER_ADDRESSES.value)
            or os.getenv("YB_MASTER_ADDRESSES")
        )
        if not master_addrs:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Master addresses not configured")
            raise ValueError("Master addresses not configured")
        
        cmd = [
            yb_admin_bin,
            "--master_addresses", master_addrs,
            "list_tables", "include_db_type", "include_table_id",
        ]
        out = subprocess.check_output(cmd, text=True)  # text=True -> str instead of bytes

        expected_prefix = f"ysql.{table_info.database}."
        expected_name = f"{table_info.schema}.{table_info.table}"

        matches = []

        for line in out.splitlines():
            # Each line looks like:
            # ysql.<db>.<schema>.<table> <table_id> <table_type>
            parts = line.strip().split()
            if len(parts) < 3:
                continue

            fq_name, table_id, _table_type = parts[0], parts[1], parts[2]

            if not fq_name.startswith(expected_prefix):
                continue

            # fq_name is "ysql.<db>.<schema>.<table>" but schema+table is one token with a dot.
            # Split "ysql.<db>." off and compare the remainder to "<schema>.<table>"
            remainder = fq_name.split('.', 2)[-1]  # "<schema>.<table>"
            if remainder == expected_name:
                matches.append(table_id)

        if not matches:
            return -1
        if len(matches) > 1:
            return -1

        return matches[0]
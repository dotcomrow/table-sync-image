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
    
    def _run_yb_admin(self, master_addrs: str, args: list[str], timeout: int = 30) -> str:
        yb_admin_bin = self.config.get(
            ConfigKeys.YUGABYTEDB.value, {}
        ).get(YugabyteDBKeys.YB_ADMIN_PATH.value, "yb-admin")
        cmd = [yb_admin_bin, "--master_addresses", master_addrs] + args
        try:
            return subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT, timeout=timeout)
        except subprocess.CalledProcessError as e:
            output = e.output or ""
            raise RuntimeError(f"yb-admin failed ({' '.join(cmd)}):\n{output}") from e

    def verify_table_covered_by_stream(self, stream_id: str, table_info: TableInfo) -> bool:
        """
        Verify if a table is covered by a given CDC stream using `get_change_data_stream_info`.
        Debezium accepts only tables that appear explicitly as `table_id:` entries on the stream.
        """
        self.logger.logMessage(
            Logging.LogLevel.DEBUG,
            "Verifying table coverage by CDC stream",
            stream_id=stream_id,
            table=table_info.to_dict(),
        )

        master_addrs = (
            self.config.get(ConfigKeys.YUGABYTEDB.value, {})
            .get(YugabyteDBKeys.MASTER_ADDRESSES.value)
            or os.getenv("YB_MASTER_ADDRESSES")
        )
        if not master_addrs:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Master addresses not configured")
            raise ValueError("Master addresses not configured")

        # 1) Resolve the table's UUID
        table_id = self.get_table_id(table_info)
        if not table_id:
            self.logger.logMessage(
                Logging.LogLevel.WARNING,
                "Could not resolve table ID; cannot verify coverage",
                table=table_info.to_dict(),
            )
            return False
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Resolved table ID", table_id=table_id, table=table_info.to_dict())

        # Helper to fetch table_ids for a stream
        def _fetch_stream_table_ids() -> set[str]:
            out = self._run_yb_admin(master_addrs, ["get_change_data_stream_info", stream_id], timeout=30)
            self.logger.logMessage(Logging.LogLevel.DEBUG, "yb-admin get_change_data_stream_info output", output=out)
            return set(re.findall(r'^\s*table_id:\s*"([0-9a-fA-F]+)"', out, flags=re.MULTILINE))

        # 2) Check explicit table_ids
        table_ids = _fetch_stream_table_ids()
        if table_id in table_ids:
            self.logger.logMessage(
                Logging.LogLevel.DEBUG,
                "Stream explicitly includes table_id",
                table_id=table_id,
                stream_table_ids=list(sorted(table_ids)),
            )
            return True

        # 3) Attempt a CDC state validation/sync (helps namespace streams pick up tables), then re-check
        try:
            sync_out = self._run_yb_admin(
                master_addrs,
                ["validate_and_sync_cdc_state_table_entries_on_change_data_stream", stream_id],
                timeout=60,
            )
            self.logger.logMessage(
                Logging.LogLevel.DEBUG,
                "Ran validate_and_sync_cdc_state_table_entries_on_change_data_stream",
                output=sync_out,
            )
            table_ids = _fetch_stream_table_ids()
            if table_id in table_ids:
                self.logger.logMessage(
                    Logging.LogLevel.DEBUG,
                    "Table joined stream after sync",
                    table_id=table_id,
                    stream_table_ids=list(sorted(table_ids)),
                )
                return True
        except Exception as e:
            self.logger.logMessage(
                Logging.LogLevel.DEBUG,
                "CDC state sync attempt failed or not supported",
                error=str(e),
            )

        # 4) Still not covered
        self.logger.logMessage(
            Logging.LogLevel.DEBUG,
            "Table is NOT explicitly part of the stream",
            table_id=table_id,
            hint=("Create a table-level stream for this table, or recreate a namespace-level stream "
                "with dynamic table addition enabled, then restart the connector."),
        )
        return False


    def get_table_id(self, table_info: TableInfo) -> Optional[str]:
        """
        Return the Yugabyte table UUID for ysql.<db>.<schema>.<table> using `yb-admin list_tables`.
        Robustly parses output across formats by deriving schema/table from the fully-qualified name
        and extracting a hex table_id token from the line.
        """
        master_addrs = (
            self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.MASTER_ADDRESSES.value)
            or os.getenv("YB_MASTER_ADDRESSES")
        )
        if not master_addrs:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Master addresses not configured")
            raise ValueError("Master addresses not configured")

        yb_admin_bin = self.config.get(
            ConfigKeys.YUGABYTEDB.value, {}
        ).get(YugabyteDBKeys.YB_ADMIN_PATH.value, "yb-admin")

        # Ask for DB type and table id; table type may appear as a 3rd token but we don't rely on it.
        out = subprocess.check_output(
            [yb_admin_bin, "--master_addresses", master_addrs, "list_tables", "include_db_type", "include_table_id"],
            text=True,
            stderr=subprocess.STDOUT,
        )

        target_db = table_info.database
        target_schema = table_info.schema
        target_table = table_info.table

        # Lines typically look like:
        #   ysql.<db>.<schema>.<table> <table_id> <table_type?>
        # Some builds can include extra kv-pairs; prefer regex extraction.
        best_match_id = None

        for line in out.splitlines():
            line = line.strip()
            if not line or not line.startswith(f"ysql.{target_db}."):
                continue

            # Extract the fully-qualified ysql name (first whitespace-separated token)
            name_token = line.split()[0]

            # Derive schema.table from the fq name
            # name_token form: ysql.<db>.<schema>.<table>
            try:
                remainder = name_token.split('.', 2)[-1]  # "<schema>.<table>"
                schema_part, table_part = remainder.split('.', 1)
            except ValueError:
                continue

            if schema_part != target_schema or table_part != target_table:
                continue

            # Extract a hex-like table id token from the rest of the line
            m = re.search(r'\b([0-9a-fA-F]{16,})\b', line[len(name_token):])
            if m:
                candidate = m.group(1)
                # Keep the first match; if multiple identical lines show up, it's the same id
                best_match_id = candidate
                break

        return best_match_id
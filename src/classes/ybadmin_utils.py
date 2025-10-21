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
    
    def _run_yb_admin(self, master_addrs: str, args: list[str], timeout: int = 20) -> str:
        yb_admin_bin = self.config.get(
            ConfigKeys.YUGABYTEDB.value, {}
        ).get(YugabyteDBKeys.YB_ADMIN_PATH.value, "yb-admin")
        cmd = [yb_admin_bin, "--master_addresses", master_addrs] + args
        try:
            return subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT, timeout=timeout)
        except subprocess.CalledProcessError as e:
            # Include full output in the log and raise a clearer error
            output = e.output or ""
            raise RuntimeError(f"yb-admin failed ({' '.join(cmd)}):\n{output}") from e

    def verify_table_covered_by_stream(self, stream_id: str, table_info: TableInfo) -> bool:
        """Verify if a table is covered by a given CDC stream using yb-admin describe_change_data_stream."""
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

        yb_admin_bin = self.config.get(
            ConfigKeys.YUGABYTEDB.value, {}
        ).get(YugabyteDBKeys.YB_ADMIN_PATH.value, "yb-admin")
        self.logger.logMessage(Logging.LogLevel.DEBUG, "yb-admin binary resolved", yb_admin_bin=yb_admin_bin)

        try:
            # 1) Resolve the table's UUID (works for YSQL)
            table_id = self.get_table_id(yb_admin_bin, table_info)
            if not table_id:
                self.logger.logMessage(
                    Logging.LogLevel.WARNING,
                    "Could not resolve table ID; cannot verify coverage",
                    table=table_info.to_dict(),
                )
                return False
            
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Resolved table ID", table_id=table_id, table=table_info.to_dict())
            # 2) Describe the stream
            out = self._run_yb_admin(master_addrs=master_addrs, args=["get_change_data_stream_info", stream_id])
            self.logger.logMessage(Logging.LogLevel.DEBUG, "yb-admin get_change_data_stream_info output", output=out)
            
            # 3) Parse: collect explicit table_ids (quoted) and namespace_id from get_change_data_stream_info
            #    Example lines in output:
            #      table_info { ... table_id: "00004000000030008000000000004001" }
            #      namespace_id: "00004000000030008000000000000000"
            table_ids = set(re.findall(r'^\s*table_id:\s*"([0-9a-fA-F]+)"', out, flags=re.MULTILINE))
            namespace_id = None

            m = re.search(r'^\s*namespace_id:\s*"([0-9a-fA-F]+)"', out, flags=re.MULTILINE)
            if m:
                namespace_id = m.group(1)

            # 4) Decide coverage
            if table_ids:
                # Stream lists specific tables; membership must include our table_id
                covered = table_id in table_ids
                self.logger.logMessage(
                    Logging.LogLevel.DEBUG,
                    "Stream explicit table_id coverage check",
                    table_id=table_id,
                    covered=covered,
                    stream_table_ids=list(sorted(table_ids)),
                )
                return covered

            # No explicit table_ids listed → likely a namespace-level stream.
            # This output does not include db_type or namespace_name, only namespace_id.
            # Without resolving namespace_id → DB name, we can't assert coverage here.
            self.logger.logMessage(
                Logging.LogLevel.DEBUG,
                "Namespace-level stream detected (no explicit table_ids)",
                namespace_id=namespace_id,
                note="Resolve namespace_id to DB name via `yb-admin list_namespaces` to confirm coverage.",
            )
            return False

        except Exception as e:
            # Ensure we surface details (including yb-admin stdout/stderr if wrapped above)
            self.logger.logMessage(
                Logging.LogLevel.ERROR,
                "Failed to verify table coverage by CDC stream",
                error=str(e),
                stream_id=stream_id,
                table=table_info.to_dict(),
            )
            raise
        
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
        expected_name = f"{table_info.table}"
        expected_schema = f"ysql_schema={table_info.schema}"

        matches = []

        for line in out.splitlines():
            # Each line looks like:
            # ysql.<db>.<schema>.<table> <table_id> <table_type>
            parts = line.strip().split()
            if len(parts) < 3:
                continue

            fq_name, table_schema, table_id = parts[0], parts[1][1:-1], parts[2][1:-1]

            if not fq_name.startswith(expected_prefix):
                continue
            
            if table_schema != expected_schema:
                continue

            # fq_name is "ysql.<db>.<schema>.<table>" but schema+table is one token with a dot.
            # Split "ysql.<db>." off and compare the remainder to "<schema>.<table>"
            remainder = fq_name.split('.', 2)[-1]  # "<schema>.<table>"
            if remainder == expected_name:
                matches.append(table_id)

        if not matches:
            return None
        if len(matches) > 1:
            return None

        return matches[0]
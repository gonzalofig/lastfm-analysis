import os
import shutil
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession


def _prepare_hadoop_runtime_home(hadoop_home: Path) -> Path:
    hadoop_home = hadoop_home.resolve()
    if os.name != "nt" or " " not in str(hadoop_home):
        return hadoop_home

    local_app_data = Path(os.environ.get("LOCALAPPDATA", hadoop_home.parent))
    runtime_home = local_app_data / "lastfm-hadoop-winutils"
    runtime_bin = runtime_home / "bin"
    runtime_bin.mkdir(parents=True, exist_ok=True)

    for source_file in (hadoop_home / "bin").glob("*"):
        if source_file.is_file():
            destination_file = runtime_bin / source_file.name
            if destination_file.exists():
                continue
            try:
                shutil.copy2(source_file, destination_file)
            except PermissionError:
                if not destination_file.exists():
                    raise

    return runtime_home


def _java_path(path: Path) -> str:
    return path.resolve().as_posix()


def _configure_local_java_home() -> None:
    if os.environ.get("JAVA_HOME"):
        return

    project_root = Path(__file__).resolve().parent.parent
    tools_dir = project_root / "tools"
    if not tools_dir.exists():
        return

    java_candidates = sorted(tools_dir.glob("**/bin/java.exe"))
    if not java_candidates:
        return

    java_home = java_candidates[0].parent.parent
    os.environ["JAVA_HOME"] = str(java_home)
    os.environ["PATH"] = f"{java_home / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"


def _configure_local_hadoop_home() -> Path | None:
    if os.environ.get("HADOOP_HOME"):
        return Path(os.environ["HADOOP_HOME"])

    project_root = Path(__file__).resolve().parent.parent
    tools_dir = project_root / "tools"
    if not tools_dir.exists():
        return None

    winutils_candidates = sorted(tools_dir.glob("**/bin/winutils.exe"))
    if not winutils_candidates:
        return None

    hadoop_home = winutils_candidates[0].parent.parent
    runtime_home = _prepare_hadoop_runtime_home(hadoop_home)
    os.environ["HADOOP_HOME"] = _java_path(runtime_home)
    os.environ["PATH"] = f"{runtime_home / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"
    return runtime_home


def _configure_local_temp_dirs() -> Path:
    project_root = Path(__file__).resolve().parent.parent
    temp_dir = project_root.parent / ".coding_challenge_tmp"

    temp_dir.mkdir(parents=True, exist_ok=True)

    os.environ["TMP"] = str(temp_dir)
    os.environ["TEMP"] = str(temp_dir)
    os.environ["TMPDIR"] = str(temp_dir)
    tempfile.tempdir = str(temp_dir)

    return temp_dir


def create_spark_session(
    app_name: str = "lastfm-case-study",
    master: str = "local[*]",
    shuffle_partitions: int = 64,
) -> SparkSession:
    _configure_local_java_home()
    hadoop_home = _configure_local_hadoop_home()
    temp_dir = _configure_local_temp_dirs()
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    vectorized_reader_enabled = "false" if os.name == "nt" else "true"

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", vectorized_reader_enabled)
        .config("spark.sql.parquet.columnarReaderBatchSize", "1024")
        .config("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.local.dir", str(temp_dir))
    )

    if hadoop_home is not None:
        hadoop_home_str = _java_path(hadoop_home)
        hadoop_bin_str = _java_path(hadoop_home / "bin")
        builder = (
            builder
            .config("spark.executorEnv.HADOOP_HOME", hadoop_home_str)
            .config(
                "spark.driver.extraJavaOptions",
                f"-Dhadoop.home.dir={hadoop_home_str} -Djava.library.path={hadoop_bin_str}",
            )
            .config(
                "spark.executor.extraJavaOptions",
                f"-Dhadoop.home.dir={hadoop_home_str} -Djava.library.path={hadoop_bin_str}",
            )
        )

    return builder.getOrCreate()

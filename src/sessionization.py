import hashlib
import shutil
import tarfile
from pathlib import Path
from urllib import request
from urllib.error import URLError

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


LASTFM_SCHEMA = T.StructType(
    [
        T.StructField("user_id", T.StringType(), False),
        T.StructField("started_at_raw", T.StringType(), False),
        T.StructField("artist_id", T.StringType(), True),
        T.StructField("artist_name", T.StringType(), True),
        T.StructField("track_id", T.StringType(), True),
        T.StructField("track_name", T.StringType(), True),
    ]
)

EXPECTED_FILE_NAME = "userid-timestamp-artid-artname-traid-traname.tsv"
PROCESSED_EVENTS_DIR_NAME = "lastfm_events_parquet"
LASTFM_ARCHIVE_NAME = "lastfm-dataset-1K.tar.gz"
LASTFM_ARCHIVE_MD5 = "a79a6808f54f73354789a9fb02cb1e41"
LASTFM_ARCHIVE_DOWNLOAD_URL = (
    "https://zenodo.org/records/6090214/files/lastfm-dataset-1K.tar.gz?download=1"
)
RAW_EXTRACT_DIR_NAME = "lastfm-dataset-1K"


def resolve_lastfm_raw_dir(project_root: Path) -> Path:
    return Path(project_root) / "data" / "raw"


def resolve_lastfm_archive_path(project_root: Path) -> Path:
    return resolve_lastfm_raw_dir(project_root) / LASTFM_ARCHIVE_NAME


def _file_md5(file_path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.md5()
    with Path(file_path).open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _expected_extract_dir(project_root: Path) -> Path:
    return resolve_lastfm_raw_dir(project_root) / RAW_EXTRACT_DIR_NAME


def download_lastfm_archive(
    project_root: Path,
    overwrite: bool = False,
    verify_md5: bool = True,
) -> Path:
    archive_path = resolve_lastfm_archive_path(project_root)
    archive_path.parent.mkdir(parents=True, exist_ok=True)

    if archive_path.exists() and not overwrite:
        if not verify_md5 or _file_md5(archive_path) == LASTFM_ARCHIVE_MD5:
            return archive_path
        archive_path.unlink()

    request_obj = request.Request(
        LASTFM_ARCHIVE_DOWNLOAD_URL,
        headers={"User-Agent": "coding-challenge-lastfm-downloader/1.0"},
    )

    try:
        with request.urlopen(request_obj, timeout=120) as response, archive_path.open("wb") as target:
            shutil.copyfileobj(response, target, length=8 * 1024 * 1024)
    except URLError as exc:
        raise RuntimeError(
            "Failed to download the Last.fm dataset archive from Zenodo."
        ) from exc

    if verify_md5 and _file_md5(archive_path) != LASTFM_ARCHIVE_MD5:
        archive_path.unlink(missing_ok=True)
        raise ValueError(
            "Downloaded Last.fm archive failed MD5 verification. "
            f"Expected {LASTFM_ARCHIVE_MD5}."
        )

    return archive_path


def extract_lastfm_archive(
    project_root: Path,
    overwrite: bool = False,
) -> Path:
    extract_dir = _expected_extract_dir(project_root)
    expected_tsv_path = extract_dir / EXPECTED_FILE_NAME
    if expected_tsv_path.exists() and not overwrite:
        return expected_tsv_path

    archive_path = resolve_lastfm_archive_path(project_root)
    if not archive_path.exists():
        archive_path = download_lastfm_archive(project_root)

    extract_dir.mkdir(parents=True, exist_ok=True)
    wanted_files = {
        "README.txt": extract_dir / "README.txt",
        "userid-profile.tsv": extract_dir / "userid-profile.tsv",
        EXPECTED_FILE_NAME: expected_tsv_path,
    }

    with tarfile.open(archive_path, "r:gz") as archive:
        members = {Path(member.name).name: member for member in archive.getmembers() if member.isfile()}
        for basename, destination_path in wanted_files.items():
            member = members.get(basename)
            if member is None:
                raise FileNotFoundError(
                    f"Expected file {basename} was not found inside {archive_path.name}."
                )
            if destination_path.exists() and not overwrite:
                continue
            extracted = archive.extractfile(member)
            if extracted is None:
                raise FileNotFoundError(
                    f"Could not read file {basename} from {archive_path.name}."
                )
            with extracted, destination_path.open("wb") as target:
                shutil.copyfileobj(extracted, target, length=8 * 1024 * 1024)

    return expected_tsv_path


def ensure_lastfm_dataset_available(
    project_root: Path,
    force_download: bool = False,
    force_extract: bool = False,
    verify_md5: bool = True,
) -> Path:
    project_root = Path(project_root)
    expected_tsv_path = _expected_extract_dir(project_root) / EXPECTED_FILE_NAME
    if expected_tsv_path.exists() and not force_download and not force_extract:
        return expected_tsv_path

    download_lastfm_archive(
        project_root,
        overwrite=force_download,
        verify_md5=verify_md5,
    )
    return extract_lastfm_archive(project_root, overwrite=force_extract)


def resolve_lastfm_input_path(project_root: Path) -> Path:
    raw_dir = resolve_lastfm_raw_dir(project_root)

    exact_match = raw_dir / EXPECTED_FILE_NAME
    if exact_match.exists():
        return exact_match

    recursive_matches = sorted(raw_dir.rglob(EXPECTED_FILE_NAME))
    if recursive_matches:
        return recursive_matches[0]

    tsv_matches = sorted(raw_dir.rglob("*.tsv"))
    if tsv_matches:
        return tsv_matches[0]

    raise FileNotFoundError(
        "Could not find the Last.fm TSV under data/raw/. "
        f"Expected something like data/raw/.../{EXPECTED_FILE_NAME}"
    )


def resolve_lastfm_parquet_path(project_root: Path) -> Path:
    return Path(project_root) / "data" / "processed" / PROCESSED_EVENTS_DIR_NAME


def parquet_dataset_exists(parquet_path: Path) -> bool:
    parquet_path = Path(parquet_path)
    return parquet_path.exists() and any(parquet_path.rglob("*.parquet"))


def suggest_spark_partitions(
    spark,
    multiplier: int = 4,
    min_partitions: int = 64,
    max_partitions: int = 512,
) -> int:
    parallelism = max(spark.sparkContext.defaultParallelism, 1)
    suggested = parallelism * multiplier
    return max(min(suggested, max_partitions), min_partitions)


def _normalize_lastfm_events(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df
        .withColumn(
            "started_at",
            F.to_timestamp("started_at_raw", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        .withColumn("song_id", F.coalesce("track_id", "track_name"))
        .filter(F.col("started_at").isNotNull())
        .filter(F.col("track_name").isNotNull())
        .select(
            "user_id",
            "started_at",
            "artist_id",
            "artist_name",
            "track_id",
            "track_name",
            "song_id",
        )
    )


def read_lastfm_events_from_tsv(spark, input_path: Path) -> DataFrame:
    return _normalize_lastfm_events(
        spark.read
        .option("sep", "\t")
        .option("header", False)
        .schema(LASTFM_SCHEMA)
        .csv(str(input_path))
    )


def stage_lastfm_events_to_parquet(
    spark,
    project_root: Path,
    overwrite: bool = False,
    parquet_partitions: int | None = None,
) -> Path:
    project_root = Path(project_root)
    parquet_path = resolve_lastfm_parquet_path(project_root)
    if parquet_dataset_exists(parquet_path) and not overwrite:
        return parquet_path

    input_path = ensure_lastfm_dataset_available(project_root)
    partition_count = parquet_partitions or suggest_spark_partitions(spark)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    (
        read_lastfm_events_from_tsv(spark, input_path)
        .repartition(partition_count, "user_id")
        .sortWithinPartitions("user_id", "started_at")
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(str(parquet_path))
    )
    return parquet_path


def load_lastfm_events(
    spark,
    project_root_or_input_path: Path,
    prefer_parquet: bool = True,
    refresh_parquet: bool = False,
    parquet_partitions: int | None = None,
) -> DataFrame:
    input_path = Path(project_root_or_input_path)
    if input_path.is_file():
        return read_lastfm_events_from_tsv(spark, input_path)

    project_root = input_path
    if prefer_parquet:
        parquet_path = resolve_lastfm_parquet_path(project_root)
        if refresh_parquet or not parquet_dataset_exists(parquet_path):
            parquet_path = stage_lastfm_events_to_parquet(
                spark,
                project_root,
                overwrite=refresh_parquet,
                parquet_partitions=parquet_partitions,
            )
        return spark.read.parquet(str(parquet_path))

    return read_lastfm_events_from_tsv(spark, resolve_lastfm_input_path(project_root))


def compute_sessions(events_df: DataFrame, gap_minutes: int = 20) -> DataFrame:
    gap_seconds = gap_minutes * 60
    prepared_df = (
        events_df
        .repartition("user_id")
        .withColumn("started_at_seconds", F.col("started_at").cast("long"))
    )

    user_window = Window.partitionBy("user_id").orderBy("started_at_seconds")
    cumulative_window = user_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    return (
        prepared_df
        .withColumn("previous_started_at_seconds", F.lag("started_at_seconds").over(user_window))
        .withColumn(
            "gap_seconds",
            F.col("started_at_seconds") - F.col("previous_started_at_seconds"),
        )
        .withColumn(
            "is_new_session",
            F.when(
                F.col("previous_started_at_seconds").isNull() | (F.col("gap_seconds") > gap_seconds),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "previous_started_at",
            F.col("previous_started_at_seconds").cast("timestamp"),
        )
        .withColumn("session_number", F.sum("is_new_session").over(cumulative_window))
        .withColumn(
            "session_id",
            F.concat_ws("-", F.col("user_id"), F.format_string("%08d", F.col("session_number"))),
        )
        .drop("started_at_seconds", "previous_started_at_seconds")
    )


def summarize_sessions(sessionized_df: DataFrame) -> DataFrame:
    return (
        sessionized_df
        .groupBy("user_id", "session_id")
        .agg(
            F.count("*").alias("track_count"),
            F.min("started_at").alias("session_start"),
            F.max("started_at").alias("session_end"),
        )
        .withColumn(
            "session_duration_seconds",
            F.greatest(
                F.lit(0),
                F.col("session_end").cast("long") - F.col("session_start").cast("long"),
            ),
        )
        .withColumn(
            "session_duration_minutes",
            F.col("session_duration_seconds") / F.lit(60.0),
        )
    )


def summarize_sessions_from_events(
    events_df: DataFrame,
    gap_minutes: int = 20,
) -> DataFrame:
    session_gap = f"{gap_minutes} minutes"
    session_number_window = Window.partitionBy("user_id").orderBy("session_start", "session_end")

    return (
        events_df
        .groupBy(
            "user_id",
            F.session_window("started_at", session_gap).alias("session_window"),
        )
        .agg(
            F.count("*").alias("track_count"),
            F.min("started_at").alias("session_start"),
            F.max("started_at").alias("session_end"),
        )
        .drop("session_window")
        .withColumn("session_number", F.row_number().over(session_number_window))
        .withColumn(
            "session_id",
            F.concat_ws("-", F.col("user_id"), F.format_string("%08d", F.col("session_number"))),
        )
        .withColumn(
            "session_duration_seconds",
            F.greatest(
                F.lit(0),
                F.col("session_end").cast("long") - F.col("session_start").cast("long"),
            ),
        )
        .withColumn(
            "session_duration_minutes",
            F.col("session_duration_seconds") / F.lit(60.0),
        )
    )


def top_longest_sessions_by_track_count(
    sessionized_df: DataFrame | None = None,
    session_summary_df: DataFrame | None = None,
    top_session_count: int = 50,
) -> DataFrame:
    summary_df = session_summary_df if session_summary_df is not None else summarize_sessions(sessionized_df)
    return (
        summary_df
        .orderBy(
            F.desc("track_count"),
            F.asc("session_start"),
            F.asc("user_id"),
            F.asc("session_id"),
        )
        .limit(top_session_count)
    )


def top_songs_from_longest_sessions(
    events_or_sessionized_df: DataFrame,
    session_summary_df: DataFrame | None = None,
    top_session_count: int = 50,
    top_song_count: int = 10,
) -> DataFrame:
    top_sessions_df = top_longest_sessions_by_track_count(
        session_summary_df=session_summary_df,
        sessionized_df=events_or_sessionized_df if "session_id" in events_or_sessionized_df.columns else None,
        top_session_count=top_session_count,
    )

    if "session_id" in events_or_sessionized_df.columns:
        return (
            events_or_sessionized_df
            .join(
                F.broadcast(top_sessions_df.select("session_id")),
                on="session_id",
                how="inner",
            )
            .groupBy("artist_name", "track_name")
            .agg(
                F.count("*").alias("play_count"),
                F.countDistinct("session_id").alias("session_count"),
                F.countDistinct("track_id").alias("distinct_track_ids"),
            )
            .orderBy(
                F.desc("play_count"),
                F.desc("session_count"),
                F.asc("artist_name"),
                F.asc("track_name"),
            )
            .limit(top_song_count)
        )

    if session_summary_df is None:
        raise ValueError(
            "session_summary_df is required when the input DataFrame does not include session_id."
        )

    top_sessions_alias = F.broadcast(
        top_sessions_df.select("user_id", "session_id", "session_start", "session_end").alias("top_sessions")
    )

    return (
        events_or_sessionized_df.alias("events")
        .join(
            top_sessions_alias,
            on=(
                (F.col("events.user_id") == F.col("top_sessions.user_id"))
                & (F.col("events.started_at") >= F.col("top_sessions.session_start"))
                & (F.col("events.started_at") <= F.col("top_sessions.session_end"))
            ),
            how="inner",
        )
        .groupBy("events.artist_name", "events.track_name")
        .agg(
            F.count("*").alias("play_count"),
            F.countDistinct("top_sessions.session_id").alias("session_count"),
            F.countDistinct("events.track_id").alias("distinct_track_ids"),
        )
        .orderBy(
            F.desc("play_count"),
            F.desc("session_count"),
            F.asc("artist_name"),
            F.asc("track_name"),
        )
        .limit(top_song_count)
    )


def top_users_by_session_count(
    sessionized_df: DataFrame | None = None,
    session_summary_df: DataFrame | None = None,
    top_user_count: int = 10,
) -> DataFrame:
    summary_df = session_summary_df if session_summary_df is not None else summarize_sessions(sessionized_df)
    return (
        summary_df
        .groupBy("user_id")
        .agg(
            F.count("*").alias("session_count"),
            F.min("session_start").alias("first_session_start"),
            F.max("session_end").alias("last_session_end"),
        )
        .orderBy(
            F.desc("session_count"),
            F.asc("user_id"),
        )
        .limit(top_user_count)
    )


def prepare_session_analysis_frames(
    spark,
    project_root: Path,
    gap_minutes: int = 20,
    refresh_parquet: bool = False,
    parquet_partitions: int | None = None,
    include_sessionized: bool = False,
) -> tuple[DataFrame, DataFrame | None, DataFrame]:
    events_df = load_lastfm_events(
        spark,
        project_root,
        prefer_parquet=True,
        refresh_parquet=refresh_parquet,
        parquet_partitions=parquet_partitions,
    )
    sessionized_df = None
    if include_sessionized:
        sessionized_df = compute_sessions(events_df, gap_minutes=gap_minutes)
    session_summary_df = summarize_sessions_from_events(
        events_df,
        gap_minutes=gap_minutes,
    ).persist(StorageLevel.DISK_ONLY)
    session_summary_df.count()
    return events_df, sessionized_df, session_summary_df

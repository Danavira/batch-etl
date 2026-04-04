from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Protocol, Sequence, Tuple
import time
import uuid
import logging

"""
A reasonably complete, practical ETL pipeline example.

Features:
- Step abstraction (Extractor / Transformer / Loader)
- Pipeline orchestration with run IDs, logging, metrics, retries
- Context manager support for resource setup/teardown
- Batch processing (streaming-style) to avoid loading everything in memory
- Optional validation hooks
- Dataclasses for configs + results
- Clear extension points for real DBs / APIs / files

This is designed to be easy to copy/paste and adapt.
"""


# ----------------------------
# Exceptions
# ----------------------------

class ETLError(Exception):
    """Base ETL error."""


class ExtractError(ETLError):
    pass


class TransformError(ETLError):
    pass


class LoadError(ETLError):
    pass


class ValidationError(ETLError):
    pass


# ----------------------------
# Protocols / Step interfaces
# ----------------------------

Record = Dict[str, Any]
Batch = List[Record]


class Step(Protocol):
    name: str

    def setup(self) -> None:
        ...

    def teardown(self) -> None:
        ...


class Extractor(Step, Protocol):
    def extract(self) -> Iterator[Batch]:
        ...


class Transformer(Step, Protocol):
    def transform_batch(self, batch: Batch) -> Batch:
        ...


class Loader(Step, Protocol):
    def load_batch(self, batch: Batch) -> int:
        """Return number of records successfully loaded."""
        ...


# ----------------------------
# Config + Results
# ----------------------------

@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 5.0
    backoff_multiplier: float = 2.0

    def compute_delay(self, attempt: int) -> float:
        # attempt starts at 1
        delay = self.base_delay_seconds * (self.backoff_multiplier ** (attempt - 1))
        return min(delay, self.max_delay_seconds)


@dataclass(frozen=True)
class PipelineConfig:
    name: str
    retries: RetryPolicy = field(default_factory=RetryPolicy)
    fail_fast: bool = True          # stop pipeline on first batch failure
    validate: bool = True           # run validators if provided
    log_level: int = logging.INFO


@dataclass
class BatchStats:
    extracted: int = 0
    transformed: int = 0
    loaded: int = 0
    failed_batches: int = 0
    batch_durations_sec: List[float] = field(default_factory=list)


@dataclass
class RunResult:
    run_id: str
    pipeline_name: str
    started_at: float
    finished_at: float
    success: bool
    stats: BatchStats
    errors: List[str] = field(default_factory=list)

    @property
    def duration_sec(self) -> float:
        return self.finished_at - self.started_at


# ----------------------------
# Validators (optional)
# ----------------------------

Validator = Callable[[Batch], None]


def require_fields(*fields: str) -> Validator:
    """Validator factory: ensure each record contains certain fields."""
    def _validate(batch: Batch) -> None:
        for i, rec in enumerate(batch):
            missing = [f for f in fields if f not in rec]
            if missing:
                raise ValidationError(f"Record {i} missing fields: {missing}")
    return _validate


# ----------------------------
# Pipeline Orchestrator
# ----------------------------

class ETLPipeline:
    """
    Orchestrates extract -> transform -> load.
    Includes:
    - context management
    - retries around batch processing
    - basic metrics
    """

    def __init__(
        self,
        config: PipelineConfig,
        extractor: Extractor,
        transformers: Sequence[Transformer],
        loader: Loader,
        validators: Optional[Sequence[Validator]] = None,
    ):
        self.config = config
        self.extractor = extractor
        self.transformers = list(transformers)
        self.loader = loader
        self.validators = list(validators) if validators else []

        self.run_id = ""
        self.logger = logging.getLogger(f"etl.{config.name}")

    def __repr__(self) -> str:
        return (
            f"ETLPipeline(name={self.config.name!r}, "
            f"extractor={self.extractor.name!r}, "
            f"transformers={[t.name for t in self.transformers]!r}, "
            f"loader={self.loader.name!r})"
        )

    # Context manager: setup/teardown resources
    def __enter__(self) -> "ETLPipeline":
        logging.basicConfig(level=self.config.log_level, format="%(asctime)s %(levelname)s %(message)s")
        self.logger.info("Setting up pipeline components...")
        self.extractor.setup()
        for t in self.transformers:
            t.setup()
        self.loader.setup()
        self.logger.info("Setup complete.")
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.logger.info("Tearing down pipeline components...")
        errors: List[str] = []
        for step in [self.loader, *reversed(self.transformers), self.extractor]:
            try:
                step.teardown()
            except Exception as e:  # don't mask original error
                errors.append(f"{step.name} teardown failed: {e!r}")
        if errors:
            self.logger.warning("Teardown errors: %s", errors)
        # Return False so exceptions propagate (typical for pipelines)
        return False

    # Make pipeline callable (nice for orchestrators/tests)
    def __call__(self) -> RunResult:
        return self.run()

    def run(self) -> RunResult:
        self.run_id = uuid.uuid4().hex
        started = time.time()
        stats = BatchStats()
        errors: List[str] = []

        self.logger.info("Starting run_id=%s pipeline=%s", self.run_id, self.config.name)
        self.logger.info("Pipeline plan: %s", self)

        try:
            for batch_index, batch in enumerate(self.extractor.extract(), start=1):
                batch_start = time.time()
                self.logger.info("Batch %d received (size=%d)", batch_index, len(batch))
                stats.extracted += len(batch)

                ok = self._process_batch_with_retries(batch_index, batch, stats, errors)

                stats.batch_durations_sec.append(time.time() - batch_start)

                if not ok:
                    stats.failed_batches += 1
                    if self.config.fail_fast:
                        self.logger.error("Fail-fast enabled; stopping after batch %d failure.", batch_index)
                        break

        except Exception as e:
            # catastrophic failure outside per-batch retries (e.g., extractor crashed)
            msg = f"Pipeline crashed: {e!r}"
            errors.append(msg)
            self.logger.exception(msg)

        finished = time.time()
        success = (len(errors) == 0) and (stats.failed_batches == 0)

        result = RunResult(
            run_id=self.run_id,
            pipeline_name=self.config.name,
            started_at=started,
            finished_at=finished,
            success=success,
            stats=stats,
            errors=errors,
        )

        self.logger.info(
            "Finished run_id=%s success=%s duration=%.2fs extracted=%d transformed=%d loaded=%d failed_batches=%d",
            result.run_id,
            result.success,
            result.duration_sec,
            result.stats.extracted,
            result.stats.transformed,
            result.stats.loaded,
            result.stats.failed_batches,
        )

        if result.errors:
            self.logger.info("Errors: %s", result.errors)

        return result

    def _process_batch_with_retries(
        self,
        batch_index: int,
        batch: Batch,
        stats: BatchStats,
        errors: List[str],
    ) -> bool:
        rp = self.config.retries

        for attempt in range(1, rp.max_attempts + 1):
            try:
                processed = self._process_batch_once(batch_index, batch, stats)
                self.logger.info("Batch %d processed successfully (attempt %d)", batch_index, attempt)
                return True
            except Exception as e:
                msg = f"Batch {batch_index} failed on attempt {attempt}/{rp.max_attempts}: {e!r}"
                errors.append(msg)
                self.logger.warning(msg)

                if attempt < rp.max_attempts:
                    delay = rp.compute_delay(attempt)
                    self.logger.info("Retrying batch %d after %.2fs...", batch_index, delay)
                    time.sleep(delay)
                else:
                    self.logger.error("Batch %d exhausted retries.", batch_index)
                    return False

        return False  # defensive

    def _process_batch_once(self, batch_index: int, batch: Batch, stats: BatchStats) -> Batch:
        # 1) Transform
        out = batch
        try:
            for t in self.transformers:
                out = t.transform_batch(out)
            stats.transformed += len(out)
        except Exception as e:
            raise TransformError(f"Transform failed in batch {batch_index}: {e}") from e

        # 2) Validate (optional)
        if self.config.validate and self.validators:
            try:
                for v in self.validators:
                    v(out)
            except Exception as e:
                raise ValidationError(f"Validation failed in batch {batch_index}: {e}") from e

        # 3) Load
        try:
            loaded_count = self.loader.load_batch(out)
            stats.loaded += loaded_count
        except Exception as e:
            raise LoadError(f"Load failed in batch {batch_index}: {e}") from e

        return out


# ----------------------------
# Example concrete implementations
# ----------------------------

class InMemoryExtractor:
    """Extracts records from a Python list, yielding batches."""
    name = "in_memory_extractor"

    def __init__(self, records: Sequence[Record], batch_size: int = 100):
        self._records = list(records)
        self._batch_size = batch_size

    def setup(self) -> None:
        # setup connections/resources here in real life
        pass

    def teardown(self) -> None:
        pass

    def extract(self) -> Iterator[Batch]:
        if self._batch_size <= 0:
            raise ExtractError("batch_size must be > 0")
        for i in range(0, len(self._records), self._batch_size):
            yield self._records[i : i + self._batch_size]


class NormalizeNamesTransformer:
    name = "normalize_names"

    def setup(self) -> None:
        pass

    def teardown(self) -> None:
        pass

    def transform_batch(self, batch: Batch) -> Batch:
        out: Batch = []
        for rec in batch:
            # copy to avoid mutating input in place (often safer)
            r = dict(rec)
            if "name" in r and isinstance(r["name"], str):
                r["name"] = " ".join(r["name"].split()).strip().title()
            out.append(r)
        return out


class AddDerivedFieldsTransformer:
    name = "add_derived_fields"

    def setup(self) -> None:
        pass

    def teardown(self) -> None:
        pass

    def transform_batch(self, batch: Batch) -> Batch:
        out: Batch = []
        for rec in batch:
            r = dict(rec)
            # example: compute "is_adult" if "age" exists
            age = r.get("age")
            if isinstance(age, int):
                r["is_adult"] = age >= 18
            out.append(r)
        return out


class InMemoryLoader:
    """Loads into a list (stand-in for DB, warehouse, etc.)."""
    name = "in_memory_loader"

    def __init__(self):
        self.rows: List[Record] = []

    def setup(self) -> None:
        pass

    def teardown(self) -> None:
        pass

    def load_batch(self, batch: Batch) -> int:
        # In real life you'd insert into DB here
        self.rows.extend(batch)
        return len(batch)


# ----------------------------
# Demo usage
# ----------------------------

if __name__ == "__main__":
    raw = [
        {"id": 1, "name": "  alice  smith ", "age": 30},
        {"id": 2, "name": "BOB jones", "age": 17},
        {"id": 3, "name": "charlie", "age": 25},
    ]

    extractor = InMemoryExtractor(raw, batch_size=2)
    transformers = [NormalizeNamesTransformer(), AddDerivedFieldsTransformer()]
    loader = InMemoryLoader()

    config = PipelineConfig(
        name="users_etl",
        retries=RetryPolicy(max_attempts=3, base_delay_seconds=0.2),
        fail_fast=True,
        validate=True,
        log_level=logging.INFO,
    )

    validators = [
        require_fields("id", "name"),  # ensures required columns exist
    ]

    with ETLPipeline(config, extractor, transformers, loader, validators) as pipe:
        result = pipe()  # __call__ -> run()

    print("\n--- RESULT ---")
    print("success:", result.success)
    print("duration_sec:", round(result.duration_sec, 3))
    print("stats:", result.stats)
    print("loaded_rows:", loader.rows)
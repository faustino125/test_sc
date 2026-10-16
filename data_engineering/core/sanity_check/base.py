import logging
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession  # type: ignore
from pyspark.sql.types import StringType, StructField, StructType  # type: ignore

logger = logging.getLogger(__name__)


class QualitySeverity(str, Enum):
    """Severity levels for data quality issues."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ValidationStatus(str, Enum):
    """Status of a validation check."""

    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"
    SKIPPED = "SKIPPED"


@dataclass
class ValidationRecord:
    """Individual validation result record."""

    nombre_dataset: str
    columna: str
    validacion: str
    severidad: str
    descripcion: str
    estado: str = "FAIL"
    fecha_ejecucion: Optional[str] = None
    _observ_end_dt: Optional[str] = None
    _observ_start_dt: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


VALIDATION_RESULT_SCHEMA = StructType(
    [
        StructField("nombre_dataset", StringType(), False),
        StructField("columna", StringType(), False),
        StructField("validacion", StringType(), False),
        StructField("descripcion", StringType(), False),
        StructField("severidad", StringType(), False),
        StructField("estado", StringType(), False),
        StructField("_observ_start_dt", StringType(), True),
        StructField("_observ_end_dt", StringType(), True),
        StructField("fecha_ejecucion", StringType(), True),
    ]
)


class BaseValidator(ABC):
    """Abstract base class for all validators."""

    def __init__(self, config: Dict[str, Any], spark: SparkSession) -> None:
        """Initialize validator with configuration and Spark session."""
        self.config = config
        self.spark = spark
        self._column_cache: Dict[str, Optional[str]] = {}

    def _resolve_column(self, p_df: DataFrame, p_col_name: str) -> Optional[str]:
        """Resolve a column name case-insensitively with caching.

        Args:
            p_df: DataFrame to search for the column
            p_col_name: Column name (case-insensitive)

        Returns:
            Actual column name if found, None otherwise
        """
        if p_col_name in self._column_cache:
            return self._column_cache[p_col_name]

        actual = next(
            (c for c in p_df.columns if c.lower() == p_col_name.lower()), None
        )
        self._column_cache[p_col_name] = actual
        return actual

    @staticmethod
    def coerce_severity(
        sev: Any, default: QualitySeverity = QualitySeverity.MEDIUM
    ) -> QualitySeverity:
        """Coerce an arbitrary value into a QualitySeverity with strict validation.

        Args:
            sev: Raw severity value (None, str or QualitySeverity)
            default: Default severity if value is None

        Returns:
            QualitySeverity enum matching the configured value.

        Raises:
            ValueError: If the value is a non-empty, unrecognized severity.
        """
        if sev is None:
            return default

        if isinstance(sev, QualitySeverity):
            return sev

        if isinstance(sev, str):
            sev_norm = sev.strip().upper()
            if sev_norm in QualitySeverity.__members__:
                return QualitySeverity[sev_norm]
            for member in QualitySeverity:
                if sev_norm == str(member.value).upper():
                    return member

            valid_opts = [s.name for s in QualitySeverity]
            raise ValueError(
                f"Invalid severity '{sev}'. Valid options: {', '.join(valid_opts)}"
            ) from None

        raise ValueError(f"QualitySeverity enum, got {type(sev).__name__}")

    def _get_severity(
        self, default: QualitySeverity = QualitySeverity.MEDIUM
    ) -> QualitySeverity:
        """Resolve severity from config or return default with strict validation.

        Args:
            default: Default severity if not found in config

        Returns:
            QualitySeverity enum matching configured value.
        """
        return self.coerce_severity(self.config.get("severity", default), default)

    def _get_config_value(self, keys: List[str], default: Any = None) -> Any:
        """Get configuration value by testing multiple possible keys in order.

        Args:
            keys: List of config keys to try in order
            default: Default value if no key is found

        Returns:
            Value from first found key, or default
        """
        for key in keys:
            value = self.config.get(key)
            if value is not None:
                return value
        return default

    def _add_validation_record(
        self,
        records: List,
        dataset_name: str,
        check_name: str,
        category: str,
        failed: bool,
        fail_description: str,
        pass_description: str,
        fail_severity: Optional["QualitySeverity"] = None,
    ) -> None:
        """Add validation record with unified PASS/FAIL logic.

        Args:
            records: List to append record to
            dataset_name: Dataset name
            check_name: Check/column name
            category: Validation category
            failed: Whether check failed
            fail_description: Description for FAIL case
            pass_description: Description for PASS case
            fail_severity: Severity for FAIL case (overrides default)
        """
        from data_engineering.core.sanity_check.utils import create_record

        if failed:
            records.append(
                create_record(
                    p_dataset_name=dataset_name,
                    p_check_name=check_name,
                    p_category=category,
                    p_severity=fail_severity or self._get_severity(),
                    p_description=fail_description,
                    p_status=ValidationStatus.FAIL,
                )
            )
        else:
            records.append(
                create_record(
                    p_dataset_name=dataset_name,
                    p_check_name=check_name,
                    p_category=category,
                    p_severity=QualitySeverity.LOW,
                    p_description=pass_description,
                    p_status=ValidationStatus.PASS,
                )
            )

    @abstractmethod
    def validate(self, p_df: DataFrame, p_dataset_name: str) -> DataFrame:
        """Execute validation on DataFrame.

        Args:
            p_df: DataFrame to validate
            p_dataset_name: Name of dataset being validated

        Returns:
            DataFrame with validation results
        """
        pass

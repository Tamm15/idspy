from typing import Any, Dict, Optional
from pyspark.sql import DataFrame
from ....core.step.base import Step
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col, lit
from pyspark.sql.types import DoubleType


@Step.needs("df")
class DropNullsSpark(Step):
    """Drop all rows that contain null values, and treat ±inf as null (Spark version)."""

    def __init__(
        self,
        df_key: str = "data.base_df",
        name: Optional[str] = None,
    ) -> None:
        super().__init__(name=name or "drop_nulls_spark")

        self.key_map = {
            "df": df_key,
        }

    def bindings(self) -> Dict[str, str]:
        return self.key_map

    def compute(self, df: DataFrame) -> Optional[Dict[str, Any]]:
        # Replace +inf and -inf with null for all numeric columns
        df_sostituiti = df.select([
            when(col(c).cast(DoubleType()) == float('inf'), lit(None))
            .when(col(c).cast(DoubleType()) == float('-inf'), lit(None))
            .otherwise(col(c)).alias(c)
            for c in df.columns
        ])

        # Rimuovi tutte le righe che contengono null
        df_pulito = df_sostituiti.dropna(how='any')


        return {"df": df_pulito}

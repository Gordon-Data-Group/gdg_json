"""GDGJson module for JSON data manipulation.

This module provides functionality to convert JSON data to polars DataFrames,
manipulate the data, and convert it back to JSON format.
"""

from io import StringIO
from typing import List, Any
from copy import deepcopy

import polars as pl

class GDGJson:
    """A class for manipulating JSON data."""

    def __init__(self, json_obj: str | dict | pl.DataFrame):
        """Initialize GDGJson with a JSON string.
        
        Args:
            json_obj (str | dict): JSON string or dictionary to be converted to DataFrame
        """
        if isinstance(json_obj, str):
            self.df = pl.read_json(StringIO(json_obj))
        elif isinstance(json_obj, dict):
            self.df = pl.DataFrame(json_obj)
        elif isinstance(json_obj, pl.DataFrame):
            self.df = json_obj
        else:
            raise ValueError(f"Invalid JSON object: {json_obj}")

    def to_json(self) -> str:
        """Convert the DataFrame back to JSON string.
        
        Returns:
            str: JSON representation of the DataFrame
        """
        return self.df.to_dicts()

    def unnest_column_as_root(self, column_name: str):
        """Expand an array column into separate root elements.
        
        Args:
            column_name (str): Name of the column containing arrays to expand
        """
        self.df = self.df.select(pl.col(column_name))
        self.df = self.__unnest_with_prefix(column_name, '')
        return self

    def unnest_column_as_rows(self, column_name: str, prefix: str = None):
        """Expand an array column into separate rows.
        
        Args:
            column_name (str): Name of the column containing arrays to expand
        """
        self.df = self.df.explode(column_name)
        self.df = self.__unnest_with_prefix(column_name, prefix)
        return self

    def rename_column(self, from_name: str, to_name: str):
        """Rename a single column.
        
        Args:
            from_name (str): Current column name
            to_name (str): New column name
        """
        self.df = self.df.rename({from_name: to_name})
        return self

    def rename_columns(self, from_names: List[str], to_names: List[str]):
        """Rename multiple columns.
        
        Args:
            from_names (List[str]): List of current column names
            to_names (List[str]): List of new column names
        """
        self.df = self.df.rename(dict(zip(from_names, to_names)))
        return self

    def drop(self, column_names: str | List[str]):
        """Remove multiple columns.
        
        Args:
            column_names (List[str]): List of column names to remove
        """
        self.df = self.df.drop(column_names)
        return self

    def duplicate_column(self, column_name: str, new_column_name: str):
        """Create a copy of an existing column with a new name.
        
        Args:
            column_name (str): Name of the column to duplicate
            new_column_name (str): Name for the new column
        """
        self.df = self.df.with_columns(pl.col(column_name).alias(new_column_name))
        return self

    def expand_column(self, column_name: str, prefix: str = None):
        """Expand a JSON column into separate columns.
        
        Args:
            column_name (str): Name of the JSON column to expand
        """
        self.df = self.__unnest_with_prefix(column_name, prefix)
        return self

    def json_normalize(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize a JSON DataFrame with a _ separator.
        
        Args:
            df (pl.DataFrame): DataFrame containing the JSON column
        """
        self.df = pl.json_normalize(df, separator='_')
        return self

    def set_constant(self, column_name: str, value: Any):
        """Set a constant value for a column.
        
        Args:
            column_name (str): Name of the column to set
            value (Any): Value to set for the column
        """
        self.df[column_name] = value
        return self

    def copy(self) -> 'GDGJson':
        """Return a copy of the DataFrame.
        
        Returns:
            GDGJson: A copy of the DataFrame
        """
        return GDGJson(self.to_json())

    def select(self, *args: str | pl.Expr, copy: bool = False) -> 'GDGJson':
        """Select columns from the DataFrame.
        
        Args:
            *args (str | pl.Expr): Column names or expressions to select
        """
        if copy:
            return self.copy().select(*args)
        else:
            self.df = self.df.select(*args)
            return self

    def __repr__(self) -> str:
        """Return a string representation of the DataFrame.
        
        Returns:
            str: String representation of the DataFrame
        """
        return self.to_json()

    def __unnest_with_prefix(self, column_name: str, prefix: str | None = None):
        """Unnest a column with a prefix.
        
        Args:
            column_name (str): Name of the column to unnest
        """
        if prefix is None:
            alias = f"{column_name}_"
        elif prefix == '':
            alias = ""
        else:
            alias = f"{prefix}_"

        return self.df.select([
            *[
                pl.col(column_name).struct.field(field.name).alias(f"{alias}{field.name}")
                for field in self.df.schema[column_name].fields
            ],
            *[pl.col(c) for c in self.df.columns if c != column_name]
        ])

    def __copy__(self):
        """Return a copy of the DataFrame.
        
        Returns:
            GDGJson: A copy of the DataFrame
        """
        return GDGJson(self.df.to_dict())

    def __deepcopy__(self, memo):
        id_self = id(self)        # memoization avoids unnecesary recursion
        _copy = memo.get(id_self)
        if _copy is None:
            _copy = type(self)(deepcopy(self.df, memo))
            memo[id_self] = _copy
        return _copy

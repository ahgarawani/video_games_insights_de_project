import pandas as pd


def create_dimension_table(df: pd.DataFrame, dimension: str) -> pd.DataFrame:
    """
    Create a dimension table by extracting unique values for a given dimension from the DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data.
        dimension (str): The name of the column representing the dimension.

    Returns:
        pd.DataFrame: A DataFrame containing unique values for the dimension and their IDs.
    """
    unique_values = (
        df[[dimension]].drop_duplicates().dropna().reset_index(drop=True)
    )
    unique_values[f'{dimension}_id'] = unique_values.index + 1
    return unique_values


def create_dim_table_query_constructor(dim_name: str) -> str:
    """
    Construct a SQL query for creating a dimension table.

    Args:
        dim_name (str): The name of the dimension.

    Returns:
        str: A SQL query string for creating the dimension table.
    """
    return f"""
        CREATE TABLE IF NOT EXISTS {dim_name}s_dim (
            {dim_name}_id SERIAL PRIMARY KEY,
            {dim_name} VARCHAR(255) NOT NULL
        );
        """


def escape_value(value: any) -> str:
    """
    Escape values for SQL insertion, handling NULLs and special characters.

    Args:
        value (any): The value to be escaped.

    Returns:
        str: The escaped value as a string suitable for SQL insertion.
    """
    if pd.isnull(value):
        return 'NULL'
    elif isinstance(value, str):
        return f"""'{value.replace("'", "''")}'"""
    else:
        return repr(value)

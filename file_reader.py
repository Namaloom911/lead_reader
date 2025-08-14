import pandas as pd
from io import StringIO


def detect_header_row(file_path):
    """
    Detect the correct header row for Excel/CSV by checking first 3 rows.
    Returns the number of rows to skip.
    """
    if file_path.endswith(".xlsx"):
        sample_df = pd.read_excel(file_path, nrows=3, header=None)
    else:
        sample_df = pd.read_csv(file_path, nrows=3, header=None)

    skiprows = 0
    for i in range(3):  # check first three rows
        non_empty_cells = sample_df.iloc[i].notna().sum()
        if non_empty_cells <= 1:  # likely merged or empty row
            skiprows += 1
        else:
            break

    return skiprows


def read_file(file_path, skiprows=None, preview_only=False):
    """
    Reads Excel or CSV file, auto-detecting header row if skiprows is None.
    preview_only: If True, loads only first 30 rows.
    """
    if skiprows is None:
        skiprows = detect_header_row(file_path)

    if preview_only:
        nrows = 30
    else:
        nrows = None

    if file_path.endswith(".xlsx"):
        df = pd.read_excel(file_path, skiprows=skiprows, nrows=nrows)
    else:
        df = pd.read_csv(file_path, skiprows=skiprows, nrows=nrows)

    return df


def read_pasted_data(raw_text):
    """
    Converts pasted tab-delimited or comma-delimited text into a DataFrame.
    Handles data copied from Excel or CSV.
    """
    # Try tab first (Excel copy-paste usually uses tabs)
    if "\t" in raw_text:
        delimiter = "\t"
    else:
        delimiter = ","

    data_io = StringIO(raw_text)
    df = pd.read_csv(data_io, delimiter=delimiter)

    return df

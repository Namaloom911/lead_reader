import pandas as pd
from typing import Tuple

def _find_col(df: pd.DataFrame, checks) -> str | None:
    cols = [str(c) for c in df.columns]
    for c in cols:
        lc = c.lower().strip()
        for pat in checks:
            if pat.startswith("*") and pat.endswith("*"):
                if pat.strip("*") in lc:
                    return c
            else:
                if lc == pat:
                    return c
    return None

def _clean_currency_to_float(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.replace(r"[^\d.\-]", "", regex=True)
        .replace({"": None})
        .astype(float)
    )

def process_sales_data(sales_df: pd.DataFrame, bats_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    sales_df = sales_df.copy()
    bats_df = bats_df.copy()
    sales_df.columns = [str(c).strip() for c in sales_df.columns]
    bats_df.columns = [str(c).strip() for c in bats_df.columns]

    # --- Column detection ---
    sales_agent_col   = _find_col(sales_df, {"agent", "agents", "agent name"})
    bats_assigned_col = next((c for c in bats_df.columns if "assigned" in c.lower()), None)
    order_id_col_sales = next((c for c in sales_df.columns if "order id" in c.lower()), None)
    order_id_col_bats  = next((c for c in bats_df.columns if "number" in c.lower()), None)
    name_col_sales     = _find_col(sales_df, {"name", "customer name"})
    name_col_bats      = next((c for c in bats_df.columns if "customer name" in c.lower()), None)
    deposit_col_sales  = next((c for c in sales_df.columns if "deposit" in c.lower()), None)
    source_col_bats    = next((c for c in bats_df.columns if "source" in c.lower()), None)

    # Validate critical columns
    missing = []
    if sales_agent_col is None:   missing.append("Sales: Agent/Agents")
    if bats_assigned_col is None: missing.append("BATS: Assigned To")
    if deposit_col_sales is None: missing.append("Sales: Deposit")
    if source_col_bats is None:   missing.append("BATS: Source")
    if name_col_sales is None or name_col_bats is None:
        missing.append("Name (Sales 'Name' and BATS 'Customer Name')")
    if missing:
        raise ValueError("Required columns missing or not detected: " + ", ".join(missing))

    # --- Standardize text fields ---
    for df_, col in [(sales_df, sales_agent_col),
                     (bats_df, bats_assigned_col),
                     (sales_df, name_col_sales),
                     (bats_df, name_col_bats)]:
        df_[col] = df_[col].astype(str).str.strip().str.lower()

    # --- Clean deposits & filter > 0 ---
    sales_df[deposit_col_sales] = _clean_currency_to_float(sales_df[deposit_col_sales])
    sales_df = sales_df[sales_df[deposit_col_sales].notna() & (sales_df[deposit_col_sales] > 0)].copy()

    # --- Aggregate deposits by Order ID first ---
    if order_id_col_sales:
        sales_df = (
            sales_df.groupby([order_id_col_sales, sales_agent_col, name_col_sales], as_index=False)
            .agg({deposit_col_sales: "sum"})
        )

    # Prepare for matching
    bats_min = bats_df[[c for c in bats_df.columns if c in {
        order_id_col_bats, name_col_bats, bats_assigned_col, source_col_bats
    }]].copy()

    sales_min = sales_df[[c for c in sales_df.columns if c in {
        order_id_col_sales, name_col_sales, sales_agent_col, deposit_col_sales
    }]].copy()

    # --- First pass: Order ID + Agent ---
    first_pass = pd.DataFrame()
    if order_id_col_sales and order_id_col_bats:
        first_pass = pd.merge(
            sales_min,
            bats_min,
            left_on=[order_id_col_sales, sales_agent_col],
            right_on=[order_id_col_bats, bats_assigned_col],
            how="inner",
            suffixes=("_sales", "_bats"),
        )

    # --- Second pass: Name + Agent (only unmatched) ---
    if not first_pass.empty and order_id_col_sales in first_pass.columns:
        matched_ids = set(first_pass[order_id_col_sales].dropna().astype(str).unique())
        unmatched_sales = sales_min[~sales_min[order_id_col_sales].astype(str).isin(matched_ids)].copy()
    else:
        unmatched_sales = sales_min.copy()

    second_pass = pd.merge(
        unmatched_sales,
        bats_min,
        left_on=[name_col_sales, sales_agent_col],
        right_on=[name_col_bats, bats_assigned_col],
        how="inner",
        suffixes=("_sales", "_bats"),
    )

    # --- Combine all matches ---
    combined = pd.concat([first_pass, second_pass], ignore_index=True)

    # --- Only keep matched leads ---
    combined = combined[combined[source_col_bats].notna()].copy()

    if combined.empty:
        final_cols = ["Order ID", "Name", "Agent", "Deposit", "Source"]
        empty = pd.DataFrame(columns=final_cols)
        summary = pd.DataFrame(columns=["Source", "Deposits", "Total_Unique_Sales"])
        return empty, summary

    # --- Final matched sales table ---
    out = pd.DataFrame({
        "Order ID": combined.get(order_id_col_sales),
        "Name":     combined[name_col_sales],
        "Agent":    combined[sales_agent_col],
        "Deposit":  combined[deposit_col_sales],
        "Source":   combined[source_col_bats],
    })

    # --- Unique sale calculation: per (Source, Name, Agent) ---
    # Store all order IDs as comma-separated string (hashable)
    out = (
        out.groupby(["Source", "Name", "Agent"], as_index=False)
        .agg({
            "Deposit": "sum",
            "Order ID": lambda x: ", ".join(sorted(set(str(v) for v in x if pd.notna(v))))
        })
    )

    # Each (Source, Name, Agent) = 1 unique sale
    summary = (
        out.groupby("Source")
        .agg(Deposits=("Deposit", "sum"), Total_Unique_Sales=("Name", "count"))
        .reset_index()
    )

    return out.reset_index(drop=True), summary.sort_values("Source", na_position="last").reset_index(drop=True)

import pandas as pd
from lead_costs import lead_costs

def compare_bats_sales(bats_df: pd.DataFrame, matched_sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create the final per-Source report table for the GUI:

    Columns:
      - Source
      - Total Leads         (from BATS)
      - Deposits            (sum of matched sales deposits)
      - Total Unique Sales  (unique sales count)
      - Lead Cost           (looked up in lead_costs, default 1.00)
      - Total Leads Cost    (Total Leads * Lead Cost)

    Requirements:
      - bats_df must have a Source column.
      - matched_sales_df must include Source, Deposit, and be deduped logically by process_sales_data().
    """
    bats = bats_df.copy()
    sales = matched_sales_df.copy()

    # Normalize column names & find 'Source'
    bats.columns = [str(c).strip() for c in bats.columns]
    sales.columns = [str(c).strip() for c in sales.columns]

    src_bats = next((c for c in bats.columns if "source" in c.lower()), None)
    src_sales = next((c for c in sales.columns if "source" in c.lower()), None)
    if src_bats is None:
        raise ValueError("No 'Source' column found in BATS data.")
    if src_sales is None:
        # If sales has zero matches, allow an empty summary gracefully
        sales["Source"] = pd.NA
        src_sales = "Source"

    # 1) Total leads per Source from BATS
    leads_summary = bats.groupby(src_bats).size().reset_index(name="Total Leads")

    # 2) From matched sales, aggregate per Source
    if "Deposit" not in sales.columns:
        # If no deposit col (shouldn't happen if process_sales_data was used), create zeroes
        sales["Deposit"] = 0.0

    deposits = sales.groupby(src_sales, dropna=False)["Deposit"].sum().rename("Deposits").reset_index()

    # Try to get unique sales count if process_sales_data prepared it,
    # otherwise infer from ("Order ID", else Name+Agent).
    if "Order ID" in sales.columns and sales["Order ID"].notna().any():
        uniq = sales[sales["Order ID"].notna()].groupby(src_sales)["Order ID"].nunique().rename("Total Unique Sales").reset_index()
    else:
        tmp = sales.copy()
        if "Name" in tmp.columns and "Agent" in tmp.columns:
            tmp["key"] = tmp["Name"].astype(str) + "||" + tmp["Agent"].astype(str)
            uniq = tmp.groupby(src_sales)["key"].nunique().rename("Total Unique Sales").reset_index()
        else:
            uniq = deposits[[src_sales]].copy()
            uniq["Total Unique Sales"] = 0

    # Merge the three pieces
    summary = (
        leads_summary
        .merge(deposits, left_on=src_bats, right_on=src_sales, how="left")
        .merge(uniq,      left_on=src_bats, right_on=src_sales, how="left")
    )

    # Clean up duplicate join keys
    for c in [src_sales + "_x", src_sales + "_y", src_sales]:
        if c in summary.columns and c != src_bats:
            summary.drop(columns=[c], inplace=True, errors="ignore")

    # Fill NA numeric
    summary["Deposits"] = summary["Deposits"].fillna(0.0).astype(float)
    summary["Total Unique Sales"] = summary["Total Unique Sales"].fillna(0).astype(int)

    # Lead Cost
    summary["Lead Cost"] = summary[src_bats].map(lead_costs).fillna(1.00).astype(float)
    summary["Total Leads Cost"] = (summary["Total Leads"].astype(float) * summary["Lead Cost"]).round(2)

    # Format Deposits for display (currency-like)
    summary["Deposits"] = summary["Deposits"].apply(lambda x: f"${x:,.0f}")

    # Final column names/order
    summary.rename(columns={src_bats: "Source"}, inplace=True)
    summary = summary[["Source", "Total Leads", "Deposits", "Total Unique Sales", "Lead Cost", "Total Leads Cost"]]

    return summary.sort_values("Source", na_position="last").reset_index(drop=True)

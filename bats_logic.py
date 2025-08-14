import pandas as pd

def clean_bats_duplicates(df: pd.DataFrame):
    """
    Standardize 'Assigned To', drop duplicates (Phone, Assigned To, Source),
    and return leads per Source.
    """
    out = df.copy()
    # Be tolerant: only transform if column present
    assigned = next((c for c in out.columns if "assigned" in str(c).lower()), None)
    if assigned:
        out[assigned] = out[assigned].astype(str).str.strip().str.lower()

    before = len(out)
    subset_cols = [c for c in out.columns if str(c) in ["Phone", "Assigned To", "Source"]]
    # If headers slightly off, try flexible find
    if len(subset_cols) < 3:
        phone_col   = next((c for c in out.columns if "phone" in str(c).lower()), None)
        assigned_col= assigned
        source_col  = next((c for c in out.columns if "source" in str(c).lower()), None)
        subset_cols = [c for c in [phone_col, assigned_col, source_col] if c is not None]

    if subset_cols:
        out = out.drop_duplicates(subset=subset_cols)

    removed = before - len(out)

    src_col = next((c for c in out.columns if "source" in str(c).lower()), None)
    if src_col is not None:
        leads_summary = out.groupby(src_col).size().reset_index(name="Total Leads")
    else:
        leads_summary = pd.DataFrame(columns=["Source", "Total Leads"])

    return out, removed, leads_summary

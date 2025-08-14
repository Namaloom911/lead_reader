import pandas as pd
from lead_costs import lead_costs

def match_bats_sales(bats_df, sales_df):
    # Match first by Order ID + Agent
    merged = pd.merge(
        bats_df,
        sales_df,
        left_on=['Number', 'Assigned To'],
        right_on=['Order ID', 'Agent'],
        how='inner'
    )

    # Match by Customer Name + Agent for sales not matched already
    unmatched_sales = sales_df[~sales_df['Order ID'].isin(merged['Order ID'])]
    name_match = pd.merge(
        bats_df,
        unmatched_sales,
        left_on=['Customer Name', 'Assigned To'],
        right_on=['Name', 'Agent'],
        how='inner'
    )

    # Combine both matches
    final_matches = pd.concat([merged, name_match], ignore_index=True)

    # Group by Source
    summary = final_matches.groupby('Source').agg(
        Deposits=('Deposit', 'sum'),
        Total_Unique_Sales=('Order ID', 'nunique')
    ).reset_index()

    # Count total leads from bats
    total_leads = bats_df.groupby('Source').size().reset_index(name='Total Leads')

    # Merge leads and sales
    summary = pd.merge(total_leads, summary, on='Source', how='left').fillna(0)

    # Add Lead Cost
    summary['Lead Cost'] = summary['Source'].map(lead_costs).fillna(1.0)

    # Total Leads Cost
    summary['Total Leads Cost'] = summary['Total Leads'] * summary['Lead Cost']

    # Format currency
    summary['Deposits'] = summary['Deposits'].apply(lambda x: f"${x:,.0f}")
    summary['Total Leads Cost'] = summary['Total Leads Cost'].round(2)

    return summary

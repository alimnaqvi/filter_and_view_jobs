import pandas as pd
from fastapi import Request

def apply_filters_from_params(df: pd.DataFrame, request: Request):
    # Apply status filter
    status = request.query_params.get("status")
    if status and status != "all":
        df = df[df['status'] == status]

    # Apply seniority filter
    seniority: list = request.query_params.getlist("seniority")
    if seniority and "all" not in seniority:
        seniority_lower = df['Role seniority'].fillna('N/A').str.lower().str
        internship_mask = (seniority_lower.contains("intern")) | (seniority_lower.contains("praktik"))
        entry_mask = seniority_lower.contains("entry")
        junior_mask = seniority_lower.contains("junior")
        mid_mask = (seniority_lower.contains("mid")) | (seniority_lower.contains("medi"))
        senior_mask = seniority_lower.contains("senior")
        unclear_mask = (seniority_lower.contains("unclear")) | (seniority_lower.contains("multiple"))
        filter_df = pd.DataFrame()
        if "internship" in seniority:
            filter_df = pd.concat([filter_df, df[internship_mask]])
        if "entry" in seniority:
            filter_df = pd.concat([filter_df, df[entry_mask]])
        if "junior" in seniority:
            filter_df = pd.concat([filter_df, df[junior_mask]])
        if "mid" in seniority:
            filter_df = pd.concat([filter_df, df[mid_mask]])
        if "senior" in seniority:
            filter_df = pd.concat([filter_df, df[senior_mask]])
        if "unclear" in seniority:
            filter_df = pd.concat([filter_df, df[unclear_mask]])
        if "other" in seniority:
            filter_df = pd.concat([filter_df, df[~internship_mask & ~entry_mask & ~entry_mask & ~junior_mask & ~mid_mask & ~senior_mask & ~unclear_mask]])
        # finally modify the original df with applied filters
        df = filter_df

    # Apply german filter
    german = request.query_params.getlist("german")
    if german and "all" not in german:
        # df['German language fluency required'] = df['German language fluency required'].fillna('N/A')
        filter_df = pd.DataFrame()
        if "intermediate" in german:
            filter_df = pd.concat([filter_df, df[df['German language fluency required'].str.lower().str.contains("intermediate")]])
        if "yes" in german:
            filter_df = pd.concat([filter_df, df[df['German language fluency required'].str.contains("Yes")]])
        if "no" in german:
            filter_df = pd.concat([filter_df, df[df['German language fluency required'].str.contains("No")]])
        if "other" in german:
            filter_df = pd.concat([filter_df, df[
                ~df['German language fluency required'].str.startswith("Yes") &
                ~df['German language fluency required'].str.startswith("No")
            ]])
        # finally modify the original df with applied filters
        df = filter_df

    return df

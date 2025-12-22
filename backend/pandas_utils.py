import pandas as pd
from fastapi import Request
from datetime import datetime, timezone

def apply_filters_from_params(df: pd.DataFrame, request: Request):
    # Apply status filter
    status = request.query_params.get("status")
    if status and status != "all":
        df = df[df['status'] == status]

    # Apply days since job saved filter
    try:
        days = float(request.query_params.get("days"))
    except Exception:
        days = 7
    df = df[pd.Timestamp(datetime.now(tz=timezone.utc)) - df['dt_last_mod_time'] <= pd.Timedelta(days=days)]

    # Apply seniority filter
    seniority: list = request.query_params.getlist("seniority")
    if seniority and "all" not in seniority:
        seniority_lower = df['Role seniority'].fillna('N/A').str.lower().str
        
        combined_mask = pd.Series([False] * len(df), index=df.index)

        internship_mask = (seniority_lower.contains("intern")) | (seniority_lower.contains("praktik"))
        entry_mask = seniority_lower.contains("entry")
        junior_mask = seniority_lower.contains("junior")
        mid_mask = (seniority_lower.contains("mid")) | (seniority_lower.contains("medi"))
        senior_mask = seniority_lower.contains("senior")
        unclear_mask = (seniority_lower.contains("unclear")) | (seniority_lower.contains("multiple"))

        if "internship" in seniority:
            combined_mask |= internship_mask
        if "entry" in seniority:
            combined_mask |= entry_mask
        if "junior" in seniority:
            combined_mask |= junior_mask
        if "mid" in seniority:
            combined_mask |= mid_mask
        if "senior" in seniority:
            combined_mask |= senior_mask
        if "unclear" in seniority:
            combined_mask |= unclear_mask
        if "other" in seniority:
            other_mask = ~(internship_mask | entry_mask | junior_mask | mid_mask | senior_mask | unclear_mask)
            combined_mask |= other_mask
        
        df = df[combined_mask]

    # Apply german filter
    german = request.query_params.getlist("german")
    if german and "all" not in german:
        german_lower = df['German language fluency required'].fillna('N/A').str.lower()
        
        combined_mask = pd.Series([False] * len(df), index=df.index)

        if "intermediate" in german:
            combined_mask |= german_lower.str.contains("intermediate")
        if "yes" in german:
            combined_mask |= german_lower.str.startswith("yes")
        if "no" in german:
            combined_mask |= german_lower.str.startswith("no")
        if "other" in german:
            other_mask = ~(german_lower.str.startswith("yes") | german_lower.str.startswith("no") | german_lower.str.contains("intermediate"))
            combined_mask |= other_mask
        
        df = df[combined_mask]

    df = df.drop_duplicates().reset_index(drop=True)

    return df

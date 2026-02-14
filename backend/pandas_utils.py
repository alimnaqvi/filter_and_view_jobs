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
        entry_mask = (seniority_lower.contains("entry")) & (seniority_lower.contains("full"))
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

    # Apply JD language filter
    jd_language_param = request.query_params.get("jd-language")
    if jd_language_param and jd_language_param != "all":
        jd_language_lower_col = df['Job description language'].fillna('N/A').str.lower()
        if jd_language_param == "english":
            df = df[jd_language_lower_col.str.contains("english")]
        elif jd_language_param == "german":
            df = df[jd_language_lower_col.str.contains("german")]
        elif jd_language_param == "other":
            df = df[~(jd_language_lower_col.str.contains("english") | jd_language_lower_col.str.contains("german"))]

    # Apply suitability filter
    suitability = request.query_params.getlist("suitability")
    if suitability and "all" not in suitability:
        suitability_lower = df['Overall suitability'].fillna('N/A').str.lower()
        
        combined_mask = pd.Series([False] * len(df), index=df.index)

        high_mask = suitability_lower.str.contains("high")
        medium_mask = (suitability_lower.str.contains("medium")) | (suitability_lower.str.contains("average"))
        low_mask = suitability_lower.str.contains("low")

        if "high" in suitability:
            combined_mask |= high_mask
        if "medium" in suitability:
            combined_mask |= medium_mask
        if "low" in suitability:
            combined_mask |= low_mask
        if "other" in suitability:
            other_mask = ~(high_mask | medium_mask | low_mask)
            combined_mask |= other_mask
        
        df = df[combined_mask]

    # Apply commute filter
    commute = request.query_params.getlist("commute")
    if commute and "all" not in commute:
        # User requested using km for approximation if hours are not specified.
        # We'll use a rough conversion of 70 km/h.
        hours_col = pd.to_numeric(df['Commute time from Heilbronn (hours)'], errors='coerce')
        dist_col = pd.to_numeric(df['Distance from Heilbronn (km)'], errors='coerce')
        effective_hours = hours_col.fillna(dist_col / 70.0)
        
        combined_mask = pd.Series([False] * len(df), index=df.index)
        
        # Define ranges
        # "remote_or_hn": 0 hours or km (using < 0.1 for float tolerance)
        is_zero = (effective_hours < 0.1)
        
        if "remote_or_hn" in commute:
            combined_mask |= is_zero
        if "less_than_1" in commute:
            combined_mask |= (effective_hours >= 0.1) & (effective_hours < 1)
        if "1_to_2" in commute:
            combined_mask |= (effective_hours >= 1) & (effective_hours < 2)
        if "2_to_3" in commute:
            combined_mask |= (effective_hours >= 2) & (effective_hours < 3)
        if "3_to_4" in commute:
            combined_mask |= (effective_hours >= 3) & (effective_hours < 4)
        if "more_than_4" in commute:
            combined_mask |= (effective_hours >= 4)
        if "other" in commute:
            combined_mask |= effective_hours.isna()
            
        df = df[combined_mask]

    # Apply source filter
    source_param = request.query_params.getlist("source")
    if source_param and "all" not in source_param:
        combined_mask = pd.Series([False] * len(df), index=df.index)

        for src in source_param:
            src_lower = src.lower()
            combined_mask |= df['source'].str.contains(src_lower)
        
        df = df[combined_mask]

    df = df.drop_duplicates().reset_index(drop=True)

    return df

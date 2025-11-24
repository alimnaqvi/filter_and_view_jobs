import pandas as pd
from fastapi import Request

def apply_filters_from_params(df: pd.DataFrame, request: Request):
    # Apply status filter
    status = request.query_params.get("status")
    if status and status != "all":
        df = df[df['status'] == status]

    # Apply seniority filter
    seniority = request.query_params.get("seniority")
    if seniority and seniority != "all":
        seniority_lower = df['Role seniority'].fillna('N/A').str.lower().str
        internship_mask = (seniority_lower.contains("intern")) | (seniority_lower.contains("praktik"))
        entry_mask = seniority_lower.contains("entry")
        junior_mask = seniority_lower.contains("junior")
        mid_mask = (seniority_lower.contains("mid")) | (seniority_lower.contains("medi"))
        senior_mask = seniority_lower.contains("senior")
        unclear_mask = (seniority_lower.contains("unclear")) | (seniority_lower.contains("multiple"))
        if seniority == "internship":
            df = df[internship_mask]
        elif seniority == "entry":
            df = df[entry_mask]
        elif seniority == "junior":
            df = df[junior_mask]
        elif seniority == "mid":
            df = df[mid_mask]
        elif seniority == "senior":
            df = df[senior_mask]
        elif seniority == "unclear":
            df = df[unclear_mask]
        else: # "Other" selected
            df = df[~internship_mask & ~entry_mask & ~entry_mask & ~junior_mask & ~mid_mask & ~senior_mask & ~unclear_mask]

    # Apply german filter
    german = request.query_params.get("german")
    if german and german != "all":
        # df['German language fluency required'] = df['German language fluency required'].fillna('N/A')
        if german == "intermediate":
            df = df[df['German language fluency required'].str.lower().str.contains("intermediate")]
        elif german == "yes":
            df = df[df['German language fluency required'].str.contains("Yes")]
        elif german == "no":
            df = df[df['German language fluency required'].str.contains("No")]
        else: # "Other" selected
            df = df[
                ~df['German language fluency required'].str.startswith("Yes") &
                ~df['German language fluency required'].str.startswith("No")
            ]

    return df

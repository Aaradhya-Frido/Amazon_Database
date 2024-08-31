import pandas as pd
import streamlit as st
from datetime import datetime
import io
import gspread
from dotenv import load_dotenv
# New added
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import os
import json
load_dotenv()
from oauth2client.service_account import ServiceAccountCredentials

# Get the JSON content from the environment variable
google_auth = st.secrets["GOOGLE_AUTH"]

# Initialize Google Sheets API using the loaded credentials
creds = ServiceAccountCredentials.from_json_keyfile_dict(google_auth)
gc = gspread.authorize(creds)


def update_google_sheet(sheet_name, df):
    """Update Google Sheets with DataFrame data by appending new data."""
    sheet = gc.open(sheet_name).sheet1
    existing_data = pd.DataFrame(sheet.get_all_records())

    if not existing_data.empty:
        # Append new data to existing data
        updated_data = pd.concat([existing_data, df], ignore_index=True)
    else:
        updated_data = df

    # Clear existing sheet and update with combined data
    sheet.clear()
    set_with_dataframe(sheet, updated_data)


##With TXT and tsv

# def read_file(uploaded_file):
#     """Read uploaded file into a DataFrame."""
#     if uploaded_file.name.endswith('.xlsx'):
#         return pd.read_excel(uploaded_file)
#     elif uploaded_file.name.endswith('.csv'):
#         return pd.read_csv(uploaded_file)
#     elif uploaded_file.name.endswith('.txt'):
#         return pd.read_csv(uploaded_file, delimiter='\t', encoding='utf-8')  # Assuming tab-separated
#     elif uploaded_file.name.endswith('.tsv'):
#         return pd.read_csv(uploaded_file, delimiter='\t', encoding='utf-8')
#     else:
#         st.error("Unsupported file format")
#         return pd.DataFrame()  # Return empty DataFrame if format is unsupported

def read_file(uploaded_file):
    """Read uploaded file into a DataFrame."""
    if uploaded_file.name.endswith('.xlsx'):
        return pd.read_excel(uploaded_file)
    elif uploaded_file.name.endswith('.csv'):
        return pd.read_csv(uploaded_file)
    else:
        st.error("Unsupported file format")
        return pd.DataFrame()  # Return empty DataFrame if format is unsupported


def read_file_asin(uploaded_file):
    """Read uploaded file into a DataFrame and rename columns if applicable."""
    # Read the file into a DataFrame
    if uploaded_file.name.endswith('.xlsx'):
        df = pd.read_excel(uploaded_file)
    elif uploaded_file.name.endswith('.csv'):
        df = pd.read_csv(uploaded_file)
    else:
        st.error("Unsupported file format")
        return pd.DataFrame()  # Return empty DataFrame if format is unsupported
    
    # Define renaming dictionaries
    rename_dict_14_day = {
        '14 Day Total Sales (₹)': 'Total Sales',
        '14 Day Total Orders (#)': 'Total Orders',
        '14 Day Total Units (#)': 'Total Units'
    }
    
    rename_dict_7_day = {
        '7 Day Total Sales (₹)': 'Total Sales',
        '7 Day Total Units (#)': 'Total Units',
        '7 Day Total Orders (#)': 'Total Orders'
    }
    
    # Rename columns based on which rename_dict matches the DataFrame columns
    if all(col in df.columns for col in rename_dict_14_day.keys()):
        df.rename(columns=rename_dict_14_day, inplace=True)
    elif all(col in df.columns for col in rename_dict_7_day.keys()):
        df.rename(columns=rename_dict_7_day, inplace=True)
    else:
        st.warning("Expected columns not found. Returning the original DataFrame.")
    
    return df

def aggregate_data_asin(df, agg_dict):
    """Aggregate data based on the provided dictionary."""
    if all(col in df.columns for col in agg_dict.keys()):
        return df.groupby(['Selected Date', 'ASIN']).agg(agg_dict).reset_index()
    else:
        st.error("One or more columns are missing for aggregation")
        return pd.DataFrame()  # Return empty DataFrame if columns are missing
       
def aggregate_data(df, agg_dict):
    """Aggregate data based on the provided dictionary."""
    if all(col in df.columns for col in agg_dict.keys()):
        return df.groupby(['Selected Date', 'Sub-Category']).agg(agg_dict).reset_index()
    else:
        st.error("One or more columns are missing for aggregation")
        return pd.DataFrame()  # Return empty DataFrame if columns are missing

def save_df_to_csv(df):
    """Save DataFrame to a CSV file in memory."""
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    return buffer

def clean_and_convert_columns(df):
    """Clean and convert columns to appropriate types."""
    
    # Define columns to convert to integer
    int_columns = [
        'Sessions - Total',
        'Sessions – Total – B2B',
        'Units Ordered',
        'Units Ordered - B2B',
        'Total Order Items',
        'Total Order Items - B2B'
    ]
    
    currency_columns = [
        'Ordered Product Sales',
        'Ordered Product Sales - B2B'
    ]
    
    # Define columns to clean
    clean_columns = [
        'Page Views - Total',
        'Page Views – Total – B2B',
        'Total Order Items',
        'Total Order Items - B2B'
    ]
    
    # Function to clean currency strings and convert to float
    def clean_currency(value):
        try:
            # Ensure the value is treated as a string
            value_str = str(value).replace('₹ ', '').replace(',', '').strip()
            # Convert the cleaned string to float
            return float(value_str)
        except (ValueError, AttributeError):
            # Return a default value or handle the error as needed
            return 0.0  # or np.nan, or log the error

    
    # Function to clean numerical strings and convert to int
    def clean_numerical(value):
        if isinstance(value, str):
            return int(value.replace(',', '').strip())
        return value
    
    # Convert columns to appropriate types
    for col in int_columns:
        df[col] = df[col].apply(clean_numerical).astype(int)
    
    for col in currency_columns:
        df[col] = df[col].apply(clean_currency).astype(float)
    
    for col in clean_columns:
        df[col] = df[col].apply(clean_numerical).astype(int)
    
    # Define the columns to aggregate
    columns_to_sum = int_columns + currency_columns
    
    # Define the aggregation functions for each column
    agg_funcs = {col: 'sum' for col in columns_to_sum}
    
    # Group by (Child) ASIN and aggregate
    aggregated_df = df.groupby('(Child) ASIN').agg(agg_funcs).reset_index()
    
    return aggregated_df

def process_files(to_file,or_file,ro_file ,sp_file, sd_file, sb_file, asin_mapping, campaign_mapping, selected_date):
    """Process and merge the uploaded files with existing data."""
    # Load existing data
    TO_existing_data = pd.DataFrame(gc.open("Business_Report_DB").sheet1.get_all_values()[1:], columns=gc.open("Business_Report_DB").sheet1.get_all_values()[0])
    OR_existing_data = pd.DataFrame(gc.open("Order_Report_DB").sheet1.get_all_values()[1:], columns=gc.open("Order_Report_DB").sheet1.get_all_values()[0])
    RO_existing_data = pd.DataFrame(gc.open("Return_Order_Report_DB").sheet1.get_all_values()[1:], columns=gc.open("Return_Order_Report_DB").sheet1.get_all_values()[0])
    sp_existing_data = pd.DataFrame(gc.open("Database_SP_DB").sheet1.get_all_values()[1:], columns=gc.open("Database_SP").sheet1.get_all_values()[0])
    sd_existing_data = pd.DataFrame(gc.open("Database_SD_DB").sheet1.get_all_values()[1:], columns=gc.open("Database_SD").sheet1.get_all_values()[0])
    sb_existing_data = pd.DataFrame(gc.open("Database_SB_DB").sheet1.get_all_values()[1:], columns=gc.open("Database_SB").sheet1.get_all_values()[0])
        


    # Read SP Files
    sp_dfs = [read_file_asin(file) for file in sp_file]
    sp_file_df = pd.concat(sp_dfs, ignore_index=True)
    
    # Read new data
    sd_dfs = [read_file_asin(file) for file in sd_file]
    sb_dfs = [read_file(file) for file in sb_file]

    # Concatenate all DataFrames for each file type
    sd_file_df = pd.concat(sd_dfs, ignore_index=True)
    sb_file_df = pd.concat(sb_dfs, ignore_index=True)

    TO_dfs = [read_file(file) for file in to_file]
    TO_file_df = pd.concat(TO_dfs, ignore_index=True)
    TO_file_df = clean_and_convert_columns(TO_file_df)

    #Order Report
    OR_dfs = [read_file(file) for file in or_file]
    OR_file_df = pd.concat(OR_dfs, ignore_index=True)
    #Return Report
    RO_dfs = [read_file(file) for file in ro_file]
    RO_file_df = pd.concat(RO_dfs, ignore_index=True)

    # Add selected date
    selected_date_str = selected_date.strftime('%Y-%m-%d')
    for df in [TO_file_df, OR_file_df,RO_file_df,sp_file_df, sd_file_df, sb_file_df]:
        df['Selected Date'] = selected_date_str

    # Merge new data with existing data
    updated_TO_files = pd.concat([TO_existing_data, TO_file_df], ignore_index=True)
    updated_OR_files = pd.concat([OR_existing_data, OR_file_df], ignore_index=True)
    updated_RO_files = pd.concat([RO_existing_data, RO_file_df], ignore_index=True)
    updated_SP_files = pd.concat([sp_existing_data, sp_file_df], ignore_index=True)
    updated_SD_files = pd.concat([sd_existing_data, sd_file_df], ignore_index=True)
    updated_SB_files = pd.concat([sb_existing_data, sb_file_df], ignore_index=True)

    # Update Google Sheets
    update_google_sheet("Business_Report_DB", updated_TO_files)
    update_google_sheet("Order_Report_DB", updated_OR_files)
    update_google_sheet("Return_Order_Report_DB", updated_RO_files)
    update_google_sheet("Database_SP_DB", updated_SP_files)
    update_google_sheet("Database_SD_DB", updated_SD_files)
    update_google_sheet("Database_SB_DB", updated_SB_files)

    # Map ASIN and generate summaries
    merged_data_TO = pd.merge(TO_file_df, asin_mapping, left_on='(Child) ASIN', right_on='ASIN', how='left')
    merged_data_SP = pd.merge(sp_file_df, asin_mapping, left_on='Advertised ASIN', right_on='ASIN', how='left')
    merged_data_SB = pd.merge(sb_file_df, campaign_mapping, left_on='Campaigns', right_on='SB Campaign Name', how='left')
    merged_data_SD = pd.merge(sd_file_df, asin_mapping, left_on='Advertised ASIN', right_on='ASIN', how='left')

    TO_summary = merged_data_TO
    OR_summary = updated_OR_files
    RO_summary = updated_RO_files
    SP_summary = aggregate_data_asin(merged_data_SP, {
        'Total Orders': 'sum',
        'Total Units': 'sum',
        'Total Sales': 'sum',
        'Spend': 'sum'
    })
    
    SB_summary = aggregate_data(merged_data_SB, {
        'Orders': 'sum',
        'Clicks': 'sum',
        'Sales(INR)': 'sum',
        'Spend(INR)': 'sum'
    })
    
    SD_summary = aggregate_data_asin(merged_data_SD, {
        'Total Orders': 'sum',
        'Total Units': 'sum',
        'Total Sales': 'sum',
        'Spend': 'sum'
    })

    return TO_summary,OR_summary,RO_summary,SP_summary, SD_summary, SB_summary

# Streamlit UI components
st.title("Amazon Processing and Uploading Page")
st.subheader("""Steps before uploading the Document!!""")
st.markdown("""
- Convert the Business Report into Numbers
- Convert the Order Report into CSV or Excel format before an upload
- Convert the Return Order Report into CSV or Excel format before an upload
- Upload UK & India Amazon reports in their particular section
""")

selected_date = st.date_input(
    "**Select a date:**",
    value=datetime.today(),  # Default date
    min_value=datetime(2000, 1, 1),  # Minimum date
    max_value=datetime(2100, 12, 31)  # Maximum date
)

# Load mappings
ASIN_Mapping = pd.read_csv("ASIN_Mapping_Report.csv")
Campaign_Mapping = pd.read_csv("Campaign_Mapping.csv")


TO_file = st.file_uploader("**Choose an Business Order Report file**", type=["xlsx", "csv", "txt", "tsv"], accept_multiple_files=True)
OR_file = st.file_uploader("**Choose an Order Report file ( With order ID's)**", type=["xlsx", "csv", "txt", "tsv"], accept_multiple_files=True)
RO_file = st.file_uploader("**Choose an Return Order Report file**", type=["xlsx", "csv", "txt", "tsv"], accept_multiple_files=True)
SP_file = st.file_uploader("**Choose an Sponsor Product file**", type=["xlsx", "csv"], accept_multiple_files=True)
SD_file = st.file_uploader("**Choose an Sponsor Display file**", type=["xlsx", "csv"], accept_multiple_files=True)
SB_file = st.file_uploader("**Choose an Sponsor Brand file**", type=["xlsx", "csv"], accept_multiple_files=True)


if st.button("Process"):
    TO_Summary,OR_summary,RO_summary,SP_summary, SD_summary, SB_summary = process_files(
        TO_file,
        OR_file,
        RO_file,
        SP_file,
        SD_file,
        SB_file,
        ASIN_Mapping,
        Campaign_Mapping,
        pd.Timestamp(selected_date)
        )

    # Save DataFrames to CSV files in memory
    TO_buffer = save_df_to_csv(TO_Summary)
    OR_buffer = save_df_to_csv(OR_summary)
    RO_buffer = save_df_to_csv(RO_summary)
    SP_buffer = save_df_to_csv(SP_summary)
    SD_buffer = save_df_to_csv(SD_summary)
    SB_buffer = save_df_to_csv(SB_summary)

    st.success("Processing completed and files have been merged.")
    st.write("Total Order files have been updated and merged.")
    st.write("Order Report files have been updated and merged.")
    st.write("Return Order Report files have been updated and merged.")
    st.write("SP files have been updated and merged.")
    st.write("SD files have been updated and merged.")
    st.write("SB files have been updated and merged.")

    # Provide download links for the summary files
    st.download_button(
        label="Download Updated Total Order Summary",
        data=TO_buffer.getvalue(),
        file_name="TO_Summary.csv",
        mime="text/csv"
     )
    st.download_button(
        label="Download Updated Order Report Summary",
        data=OR_buffer.getvalue(),
        file_name="OR_Summary.csv",
        mime="text/csv"
     )
    st.download_button(
        label="Download Updated Report Order Summary",
        data=RO_buffer.getvalue(),
        file_name="Ro_Summary.csv",
        mime="text/csv"
     )
    st.download_button(
        label="Download Updated SP Summary",
        data=SP_buffer.getvalue(),
        file_name="SP_Summary.csv",
        mime="text/csv"
     )
    st.download_button(
        label="Download Updated SD Summary",
        data=SD_buffer.getvalue(),
        file_name="SD_Summary.csv",
        mime="text/csv"
    )
    st.download_button(
        label="Download Updated SB Summary",
        data=SB_buffer.getvalue(),
        file_name="SB_Summary.csv",
        mime="text/csv"
    )

    # Update Google Sheets with the summary DataFrames
    update_google_sheet("Total_Order_Summary_DB", TO_Summary)
    update_google_sheet("Daily_Order_Summary_DB", OR_summary)
    update_google_sheet("Daily_Return_Summary_DB", RO_summary)
    update_google_sheet("SP_Summary_DB", SP_summary)
    update_google_sheet("SD_Summary_DB", SD_summary)
    update_google_sheet("SB_Summary_DB", SB_summary)

else:
    st.warning("Please upload all required files (Total Order,SP, SD, SB).")

# %%
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib3.exceptions import InsecureRequestWarning
import warnings
import os

warnings.filterwarnings("ignore", category=InsecureRequestWarning)


# %%


def fetch_apex_data(
    base_url="http://190.107.120.16:8080/apex/wwv_flow.show", start_row=1, max_rows=50
):
    headers = {
        "Accept": "*/*",
        "Accept-Language": "es-419,es;q=0.9",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded",
        "Cookie": "WWV_CUSTOM-F_1132023184885167_104=ORA_WWV-OEf6yjabcQl+gD/6MhHdXC0G",
        "DNT": "1",
        "Origin": "http://190.107.120.16:8080",
        "Referer": "http://190.107.120.16:8080/apex/f?p=104:3005:::NO:3005:F104_E1D_POLICY,P3005_TIPO:E1D,ORD",
    }

    data = {
        "p_request": "APXWGT",
        "p_instance": "8105332068485",
        "p_flow_id": "104",
        "p_flow_step_id": "3005",
        "p_widget_num_return": max_rows,
        "p_widget_name": "worksheet",
        "p_widget_mod": "ACTION",
        "p_widget_action": "PAGE",
        "p_widget_action_mod": f"pgR_min_row={start_row}max_rows={max_rows}rows_fetched={max_rows}",
        "x01": "1103022931242056",
        "x02": "1104926756242155",
    }

    try:
        response = requests.post(base_url, headers=headers, data=data, verify=False)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {str(e)}")
        return None


def parse_table_data(html_content):
    """Parse the HTML response and extract table data with URLs"""
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        outer_table = soup.find("table")

        if not outer_table:
            print("No outer table found")
            return None

        inner_table = outer_table.find("table")
        if not inner_table:
            print("No inner table found")
            return None

        # Extract data from rows
        rows_data = []
        for row in inner_table.find_all("tr", class_=["odd", "even"]):
            row_dict = {}

            # Get all td elements
            tds = row.find_all("td")
            if len(tds) >= 5:  # Ensure we have all expected columns
                # Extract link from last td
                last_td = tds[-1]
                link_tag = last_td.find("a")
                if link_tag and "href" in link_tag.attrs:
                    row_dict["document_url"] = (
                        "http://190.107.120.16:8080/apex/" + link_tag["href"]
                    )
                else:
                    row_dict["document_url"] = ""

                # Extract other columns
                row_dict["year"] = tds[1].get_text(strip=True)
                row_dict["number"] = tds[2].get_text(strip=True)
                row_dict["extract"] = tds[3].get_text(strip=True)

            if row_dict:
                rows_data.append(row_dict)

        # Convert to DataFrame
        if rows_data:
            df = pd.DataFrame(rows_data)
            return df

        return None
    except Exception as e:
        print(f"Error parsing table: {str(e)}")
        return None


def scrape_and_save_pages(max_rows=50, output_dir="page_data"):
    """Scrape pages and save each to a separate CSV file"""
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    start_row = 1
    page_num = 1
    saved_files = []

    while True:
        print(
            f"Fetching page {page_num} (rows {start_row} to {start_row + max_rows - 1})"
        )

        # Fetch data for current page
        response_html = fetch_apex_data(start_row=start_row, max_rows=max_rows)
        if not response_html:
            break

        # Parse the data
        df = parse_table_data(response_html)
        if df is None or df.empty:
            break

        # Save current page to CSV
        page_file = os.path.join(output_dir, f"page_{page_num}.csv")
        df.to_csv(page_file, index=False)
        saved_files.append(page_file)
        print(f"Saved page {page_num} to {page_file}")

        # Check if we've received fewer rows than requested (last page)
        if len(df) < max_rows:
            break

        start_row += max_rows
        page_num += 1
        time.sleep(1)  # Polite delay between requests

    return saved_files


def combine_csv_files(file_list, output_file="combined_data.csv"):
    """Combine multiple CSV files into a single file"""
    if not file_list:
        print("No files to combine")
        return

    dfs = []
    for file in sorted(file_list):  # Sort to ensure correct order
        try:
            df = pd.read_csv(file)
            dfs.append(df)
            print(f"Added {file} to combination ({len(df)} rows)")
        except Exception as e:
            print(f"Error reading {file}: {str(e)}")

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df.to_csv(output_file, index=False)
        print(f"\nSuccessfully combined {len(dfs)} files into {output_file}")
        print(f"Total rows: {len(combined_df)}")
        return combined_df
    return None


# %%


# Create directory for individual page files
output_dir = "page_data"

# Scrape and save individual pages
saved_files = scrape_and_save_pages(max_rows=50, output_dir=output_dir)

# Combine all pages into a single file
if saved_files:
    combine_csv_files(saved_files, "combined_data.csv")

# %%

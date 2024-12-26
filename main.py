# %%
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib3.exceptions import InsecureRequestWarning
import warnings
import os
from typing import List, Dict
import ssl

warnings.filterwarnings("ignore", category=InsecureRequestWarning)


async def fetch_apex_data_async(
    session: aiohttp.ClientSession, start_row: int, max_rows: int = 50
) -> Dict:
    """Async function to fetch data from APEX"""
    print(f"Fetching data for rows {start_row} to {start_row + max_rows - 1}")
    url = "http://190.107.120.16:8080/apex/wwv_flow.show"

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
        "p_widget_num_return": str(max_rows),
        "p_widget_name": "worksheet",
        "p_widget_mod": "ACTION",
        "p_widget_action": "PAGE",
        "p_widget_action_mod": f"pgR_min_row={start_row}max_rows={max_rows}rows_fetched={max_rows}",
        "x01": "1103022931242056",
        "x02": "1104926756242155",
    }

    try:
        async with session.post(url, headers=headers, data=data, ssl=False) as response:
            return {
                "start_row": start_row,
                "content": await response.text(),
                "status": response.status,
            }
    except Exception as e:
        print(f"Error fetching data for row {start_row}: {str(e)}")
        return {"start_row": start_row, "content": None, "status": None}


def parse_table_data(html_content: str) -> pd.DataFrame:
    """Parse the HTML response and extract table data"""
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        outer_table = soup.find("table")

        if not outer_table:
            return None

        inner_table = outer_table.find("table")
        if not inner_table:
            return None

        rows_data = []
        for row in inner_table.find_all("tr", class_=["odd", "even"]):
            row_dict = {}
            tds = row.find_all("td")

            if len(tds) >= 5:
                last_td = tds[-1]
                link_tag = last_td.find("a")
                if link_tag and "href" in link_tag.attrs:
                    row_dict["document_url"] = (
                        "http://190.107.120.16:8080/apex/" + link_tag["href"]
                    )
                else:
                    row_dict["document_url"] = ""

                row_dict["year"] = tds[1].get_text(strip=True)
                row_dict["number"] = tds[2].get_text(strip=True)
                row_dict["extract"] = tds[3].get_text(strip=True)

            if row_dict:
                rows_data.append(row_dict)

        return pd.DataFrame(rows_data) if rows_data else None
    except Exception as e:
        print(f"Error parsing table: {str(e)}")
        return None


async def process_batch(
    session: aiohttp.ClientSession, start_rows: List[int], max_rows: int
) -> List[pd.DataFrame]:
    """Process a batch of requests"""
    tasks = [
        fetch_apex_data_async(session, start_row, max_rows) for start_row in start_rows
    ]
    responses = await asyncio.gather(*tasks)

    dataframes = []
    for response in responses:
        if response["content"]:
            df = parse_table_data(response["content"])
            if df is not None:
                dataframes.append(df)

    return dataframes


async def scrape_data_async(
    max_rows: int = 50, batch_size: int = 10, output_dir: str = "page_data"
):
    """Main async function to scrape data in batches"""
    os.makedirs(output_dir, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        start_row = 1
        batch_num = 1
        all_files = []

        while True:
            print(f"\nProcessing batch {batch_num}")

            # Generate start rows for this batch
            start_rows = list(
                range(start_row, start_row + (batch_size * max_rows), max_rows)
            )

            # Process batch
            dataframes = await process_batch(session, start_rows, max_rows)

            if not dataframes:
                print("No more data to fetch")
                break

            # Save each dataframe in the batch
            for i, df in enumerate(dataframes):
                if df is not None and not df.empty:
                    file_name = f"page_{batch_num}_{i+1}.csv"
                    file_path = os.path.join(output_dir, file_name)
                    df.to_csv(file_path, index=False)
                    all_files.append(file_path)
                    print(f"Saved {file_name} with {len(df)} rows")

            # Check if we got less data than expected
            if any(len(df) < max_rows for df in dataframes if df is not None):
                break

            start_row += batch_size * max_rows
            batch_num += 1

            # Small delay between batches
            await asyncio.sleep(1)

        return all_files


def combine_csv_files(
    file_list: List[str], output_file: str = "combined_data.csv"
) -> pd.DataFrame:
    """Combine multiple CSV files into a single file"""
    if not file_list:
        print("No files to combine")
        return None

    dfs = []
    for file in sorted(file_list):
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
async def main():
    output_dir = "page_data"
    saved_files = await scrape_data_async(
        max_rows=50, batch_size=10, output_dir=output_dir
    )

    if saved_files:
        combine_csv_files(saved_files, "combined_data.csv")


# %%
if __name__ == "__main__":
    asyncio.run(main())

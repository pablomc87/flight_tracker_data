import requests
from bs4 import BeautifulSoup
import json
import time
from tqdm import tqdm

base_url = "https://www.avcodes.co.uk/details.asp?ID="


def fetch_and_process(id):
    max_retries = 5
    retries = 0
    while retries < max_retries:
        url = f"{base_url}{id}"
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find(
                "table", {"class": "table table-striped table-bordered border-dark"}
            )
            if table:
                rows = table.find_all("tr")
                data = {}
                data[id] = []
                for row in rows:
                    columns = row.find_all("td")
                    data[id].append([column.text.strip() for column in columns])
                return data
        elif response.status_code == 500:
            break
        else:
            print(
                f"Failed to retrieve data for ID {id}. Error code: {response.status_code}. Retrying {retries}/{max_retries} in 5 seconds..."
            )
            retries += 1
            time.sleep(5)
        if max_retries == retries:
            data = {}
            data[id] = "Failed"
            return data


def scrape_avcodes(start_id, end_id):
    all_data = {}
    for id in tqdm(range(start_id, end_id + 1)):
        data = fetch_and_process(id)
        if data:
            all_data.update(data)

    print("Saving data to JSON...")
    with open("scraped_data.json", "w") as f:
        json.dump(all_data, f, indent=4)


# Example usage
if __name__ == "__main__":
    scrape_avcodes(1, 40000)

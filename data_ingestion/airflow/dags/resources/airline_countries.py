import json

cleaned_data = {}
with open("scraped_data.json") as f:
    data = json.load(f)

    for item in data:
        data[item] = [y for x in data[item] for y in x if len(y) > 0]
        airline_icao = data[item][3].split(":")[1]
        if len(airline_icao) > 0:
            cleaned_data[airline_icao] = {
                "name": data[item][0],
                "country": data[item][7].split(":")[1],
            }

with open("scraped_data.json", "w") as f:
    json.dump(cleaned_data, f, indent=4)

import requests
sparql = """SELECT ?item ?itemLabel WHERE {
  ?item wdt:P131+ wd:Q649 .
  ?item wdt:P31 wd:Q192078 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ru" . }
}"""
r = requests.get("https://query.wikidata.org/sparql", params={"query": sparql, "format": "json"}, headers={"User-Agent":"UrbanPulse"})
items = r.json().get("results", {}).get("bindings", [])
print(f"Total Q192078: {len(items)}")
print([x["itemLabel"]["value"] for x in items[:10]])

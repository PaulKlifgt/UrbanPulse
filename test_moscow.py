import requests
sparql = """SELECT ?type ?typeLabel (COUNT(?item) AS ?count) WHERE {
  ?item wdt:P131+ wd:Q649 .
  ?item wdt:P31 ?type .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ru" . }
} GROUP BY ?type ?typeLabel ORDER BY DESC(?count) LIMIT 20"""
r = requests.get("https://query.wikidata.org/sparql", params={"query": sparql, "format": "json"}, headers={"User-Agent":"UrbanPulse"})
for row in r.json().get("results", {}).get("bindings", []):
    print(row["typeLabel"]["value"] + ": " + row["count"]["value"])

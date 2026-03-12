import requests
import time
sparql = """SELECT ?item ?itemLabel ?coord WHERE {
  ?city rdfs:label "Москва"@ru .
  ?city wdt:P31/wdt:P279* wd:Q515 .
  ?item wdt:P131+ ?city .
  ?item wdt:P31 ?type .
  VALUES ?type { wd:Q123705 wd:Q15715406 wd:Q3957 wd:Q12813115 wd:Q19953632 wd:Q4286337 wd:Q253019 wd:Q192078 wd:Q3413999 wd:Q4388406 }
  OPTIONAL { ?item wdt:P625 ?coord . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ru,en" . }
} LIMIT 300"""
start = time.time()
r = requests.get("https://query.wikidata.org/sparql", params={"query": sparql, "format": "json"}, headers={"User-Agent":"UrbanPulse/2.0"}, timeout=15)
end = time.time()
items = r.json().get("results", {}).get("bindings", [])
print(f"Time: {end-start:.2f}s, count: {len(items)}")

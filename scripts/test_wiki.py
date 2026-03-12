import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from run_pipeline import _wiki_candidate_titles, _wiki_parse_wikitext, _wiki_extract_zone_names

def test_wiki(city):
    titles = _wiki_candidate_titles(city)
    if not titles:
        titles = [f"Районы {city}", f"Список районов {city}", f"Микрорайоны {city}"]
    
    print(f"Cand titles for {city}: {titles}")
    for title in titles:
        wt = _wiki_parse_wikitext(title)
        if wt:
            names = _wiki_extract_zone_names(wt, city=city, page_title=title)
            print(f"Found in {title}: {names}")

if __name__ == "__main__":
    test_wiki("Иркутск")
    test_wiki("Хабаровск")
    test_wiki("Ульяновск")

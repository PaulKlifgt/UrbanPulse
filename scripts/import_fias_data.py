#!/usr/bin/env python3
import os
import re
import sqlite3
import argparse

def parse_fias(sql_file, db_file):
    if os.path.exists(db_file):
        os.remove(db_file)
        
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    
    cur.execute('''
        CREATE TABLE districts (
            aoid TEXT PRIMARY KEY,
            formalname TEXT,
            regioncode TEXT,
            shortname TEXT,
            aolevel INTEGER,
            parentguid TEXT
        )
    ''')
    
    values_pattern = re.compile(
        r"\("
        r"'(?P<aoid>[^']*)',"               # 1 aoid 
        r"'(?P<formalname>[^']*)',"         # 2 formalname
        r"'(?P<regioncode>[^']*)',"         # 3 regioncode
        r"'[^']*',"                         # 4 autocode
        r"'[^']*',"                         # 5 areacode
        r"'[^']*',"                         # 6 citycode
        r"'[^']*',"                         # 7 ctarcode
        r"'[^']*',"                         # 8 placecode
        r"'[^']*',"                         # 9 streetcode
        r"'[^']*',"                         # 10 extrcode
        r"'[^']*',"                         # 11 sextcode
        r"'[^']*',"                         # 12 offname
        r"'[^']*',"                         # 13 postalcode
        r"'[^']*',"                         # 14 ifnsfl
        r"'[^']*',"                         # 15 terrifnsfl
        r"'[^']*',"                         # 16 ifnsul
        r"'[^']*',"                         # 17 terrifnsul
        r"'[^']*',"                         # 18 okato
        r"'[^']*',"                         # 19 oktmo
        r"'[^']*',"                         # 20 updatedate
        r"'(?P<shortname>[^']*)',"          # 21 shortname
        r"(?P<aolevel>\d+),"                # 22 aolevel
        r"'(?P<parentguid>[^']*)',"         # 23 parentguid
        r"'[^']*',"                         # 24 aoguid
        r"'[^']*',"                         # 25 previd
        r"'[^']*',"                         # 26 nextid
        r"'[^']*',"                         # 27 code
        r"'[^']*',"                         # 28 plaincode
        r"(?P<actstatus>\d+),"              # 29 actstatus
        r"\d+,"                             # 30 centstatus
        r"\d+,"                             # 31 operstatus
        r"\d+,"                             # 32 currstatus
        r"'[^']*',"                         # 33 startdate
        r"'[^']*',"                         # 34 enddate
        r"'[^']*'"                          # 35 normdoc
        r"\)"
    )

    file_size = os.path.getsize(sql_file)
    inserted = 0
    read_bytes = 0

    print(f"Parsing {sql_file} to {db_file}...")
    with open(sql_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            line_bytes = len(line.encode('utf-8'))
            read_bytes += line_bytes
            
            if i % 500 == 0:
                print(f"\rProgress: {read_bytes / file_size * 100:.1f}%, inserted {inserted} rows", end="")
            
            if not line.startswith("INSERT INTO"):
                continue

            matches = values_pattern.finditer(line)
            batch = []
            for match in matches:
                # Only insert actual records (actstatus == 1)
                # and relevant aolevels (1=region, 3=district, 4=city, 5=intracity, 6=settlement)
                if match.group('actstatus') == '1':
                    level = int(match.group('aolevel'))
                    if level in (1, 3, 4, 5, 6):
                        batch.append((
                            match.group('aoid'),
                            match.group('formalname'),
                            match.group('regioncode'),
                            match.group('shortname'),
                            level,
                            match.group('parentguid')
                        ))
            
            if batch:
                cur.executemany('''
                    INSERT INTO districts (aoid, formalname, regioncode, shortname, aolevel, parentguid)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', batch)
                inserted += len(batch)
                
    print(f"\nCreating indexes on {inserted} rows...")
    cur.execute("CREATE INDEX idx_formalname_level ON districts(formalname, aolevel)")
    cur.execute("CREATE INDEX idx_parentguid ON districts(parentguid)")
    cur.execute("CREATE INDEX idx_region_level_short ON districts(regioncode, aolevel, shortname)")
    
    conn.commit()
    conn.close()
    print("Done!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql", default="/Users/arturboev/Documents/urbanpulse/UrbanPulse/fias_addrobj_data.sql")
    parser.add_argument("--db", default="/Users/arturboev/Documents/urbanpulse/UrbanPulse/data/fias.db")
    args = parser.parse_args()
    
    os.makedirs(os.path.dirname(args.db), exist_ok=True)
    parse_fias(args.sql, args.db)

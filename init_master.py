import json
import pandas as pd
from pathlib import Path
from datetime import date

ROOT = Path(__file__).parent.absolute()
EXCEL = ROOT / "6_0_BSE_1000_Sector_Allocation___Results_Schedule.xlsx"
MASTER_FILE = ROOT / "company_master.json"

def init_master():
    print(f"Initializing Company Master from {EXCEL.name}...")
    try:
        # Load Universe
        df = pd.read_excel(EXCEL, sheet_name='BSE2000', header=None, skiprows=2)
        
        master = {}
        # Col J(9)=Subgroup, Col L(11)=NSE Symbol
        for _, row in df.iterrows():
            sym = str(row[11]).strip().upper()
            industry = str(row[9]).strip()
            if sym and sym != 'NAN' and industry != 'NAN':
                master[sym] = {
                    "industry": industry,
                    "last_result": None,
                    "upcoming_meeting": None,
                    "purpose": None,
                    "updated_at": date.today().isoformat()
                }
        
        # Load existing results if any to backfill
        # (This will be done by the runner later)
        
        with open(MASTER_FILE, "w") as f:
            json.dump(master, f, indent=2)
            
        print(f"Master initialized with {len(master)} companies.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    init_master()

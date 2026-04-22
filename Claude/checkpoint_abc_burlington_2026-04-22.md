# Checkpoint: Burlington ABC Code Update — 2026-04-22

## Task
Update ABC codes for 18 items in Oracle Fusion DEV13 inventory org `3165_US_BUR_MFG` (Burlington)
using data from `Burlington_Strat_Change_04212026.xlsx`.

## Items to Update

| Item | Old ABC | New ABC |
|------|---------|---------|
| 02040RIP4-SPRAY | C | D |
| 114-15721-01 | B | P |
| 212-00011-04 | C | D |
| 22459YXF-SPRAY | C | D |
| 298-00114-93 | C | D |
| 30001167 | C | D |
| 30003263 | C | D |
| 30004360 | C | D |
| 398-02077-73 | C | D |
| 398-11000-19 | C | D |
| 398-11000-22 | C | D |
| 398-14000-21 | C | D |
| 398-20000-23 | C | D |
| 398-20000-25 | C | D |
| 398-20000-37 | C | D |
| 398-20000-39 | C | D |
| 399-20442-36 | A | P |
| 60-66593-01 (ELEC/M.02.06C4) | C | D |

## Status
Script written and ready. No items have been updated yet.

## Approach
Playwright UI automation via SSO session (`oracle_session.json`).

**Navigation:** FuseWelcome → hamburger → Product Management → Product Information Management → Manage Items

**Per-item workflow:**
1. Search item by number
2. Select the `3165_US_BUR_MFG` row (click non-link cell to avoid navigating away)
3. Manage Item Mass Changes → Edit Item Attributes
4. Wizard step 1: expand "Edit Columns", add ABC Class attribute
5. Click Next → step 2: set new ABC value
6. Submit

## Scripts
- **`pipeline/update_abc_ui_burlington.py`** — Main automation script (Playwright UI)
- **`pipeline/update_abc_codes_burlington.py`** — REST API approach (blocked by OWSM auth)

## Key Technical Notes
- Oracle FSCM REST API (`/fscmRestApi`) requires Basic/OAuth auth, not SSO cookies → blocked
- PIM UI approach works with cached SSO session
- BUR row is typically the second row in search results (GMO is first)
- Row selection: click Description column cell (not the item hyperlink link)
- Mass changes button has a separate dropdown arrow to the right of the button text
- Screenshots saved to `pipeline/abc_screenshots/` during run

## Oracle Instance
- Host: `https://fa-eqtl-dev13-saasfaprod1.fa.ocs.oraclecloud.com`
- Org: `3165_US_BUR_MFG`
- Session: `pipeline/oracle_session.json`

## Run Commands
```bash
cd "C:\Users\agard\OneDrive - astecindustries.com\VS Code\pipeline"

# Explore mode: navigate to first item and take screenshots at each step
python update_abc_ui_burlington.py --explore

# Dry run: just print the changes
python update_abc_ui_burlington.py --dry-run

# Run a single item
python update_abc_ui_burlington.py --item 02040RIP4-SPRAY

# Full run
python update_abc_ui_burlington.py
```

## Next Step
Run with `--explore` to inspect the "Edit Item Attributes" wizard step 2 screenshot
(`abc_screenshots/step2_02040RIP4-SPRAY.png`) and confirm the ABC Class field location,
then run fully.

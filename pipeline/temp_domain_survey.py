"""Survey all 106 Grok conversations for domain breadth."""
import json, pathlib, re

PATH = pathlib.Path("docs/Introduction to SCB/ttl/30d/export_data"
                    "/9826f3fc-ec86-4751-8129-de43d903e27e/prod-grok-backend.json")
raw = json.loads(PATH.read_text("utf-8"))
convs = raw["conversations"]

for i, cw in enumerate(convs):
    responses = cw.get("responses", [])
    human_msgs = [r["response"]["message"] for r in responses
                  if r.get("response", {}).get("sender") == "human"]
    asst_msgs  = [r["response"]["message"] for r in responses
                  if r.get("response", {}).get("sender") == "assistant"]
    first_q = (human_msgs[0] if human_msgs else "")[:80].replace("\n", " ")
    full = " ".join(human_msgs + asst_msgs).lower()

    sc_kw    = sum(1 for k in ["supply chain","inventory","vendor","erp","oracle","abc","cycle count","otd","procurement","freight","astec","scb"] if k in full)
    phys_kw  = sum(1 for k in ["quantum","atom","nuclear","energy","particle","relativit","electromagnetic","thermodynamic"] if k in full)
    bio_kw   = sum(1 for k in ["biochem","enzyme","protein","dna","rna","atp","nad","metabolism","cellular","genetic"] if k in full)
    mat_kw   = sum(1 for k in ["material","alloy","crystal","polymer","nanotechnology","semiconductor","lithium","metal"] if k in full)
    ai_kw    = sum(1 for k in ["machine learning","neural","algorithm","optimization","inference","model","transformer"] if k in full)
    cos_kw   = sum(1 for k in ["cosmolog","universe","galactic","dark matter","dark energy","multiverse","spacetime","black hole","stellar"] if k in full)
    career_kw= sum(1 for k in ["resume","cover letter","interview","job offer","linkedin","hiring","salary"] if k in full)

    print(f"[{i:3d}] SC={sc_kw} PHY={phys_kw} BIO={bio_kw} MAT={mat_kw} AI={ai_kw} COS={cos_kw} CAR={career_kw}  '{first_q}'")

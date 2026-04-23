import re

with open('pipeline/src/brain/dynamic_insight.py', 'r', encoding='utf-8') as f:
    code = f.read()
    
# Replace the html_code string entirely with one that has NO indentation.
new_code = re.sub(r'        html_code = f"""\n.*?\n        """', r'''        html_code = f"""
<style>
/* Override streamlit layout overflow to allow tooltip to escape bounds */
div[data-testid="stVerticalBlock"], 
div[data-testid="stVerticalBlockBorderWrapper"], 
div[data-testid="stMarkdownContainer"] {{
    overflow: visible !important;
}}

.dbi-container {{
    background-color: #f0f2f6; 
    border-left: 4px solid #0068c9;
    padding: 1rem;
    border-radius: 0.5rem;
    margin-bottom: 1rem;
    position: relative;
    cursor: pointer;
    color: #31333f;
    font-family: "Source Sans Pro", sans-serif;
    z-index: 999999 !important;
}}
.dbi-tooltip {{
    visibility: hidden;
    background-color: #ffffff;
    color: #31333f;
    border: 1px solid #dcdcdc;
    border-radius: 6px;
    padding: 15px;
    position: absolute;
    z-index: 999999 !important;
    top: 100%;
    left: 0;
    width: 90%;
    box-shadow: 0px 10px 30px rgba(0,0,0,0.15);
    opacity: 0;
    transition: opacity 0.2s, visibility 0.2s;
    font-size: 0.9em;
}}
.dbi-container:hover .dbi-tooltip {{
    visibility: visible;
    opacity: 1;
}}
.dbi-text {{
    font-size: 1em;
}}
</style>

<div class="dbi-container">
    <div class="dbi-text">🧠 <b>Dynamic Brain Insight ({page_name}):</b><br>{insight}</div>
    <div class="dbi-tooltip">
        <b>Relational Parameters Read by Brain:</b><br><br>
        <ul style="margin: 0; padding-left: 20px;">
            {ctx_html_items}
        </ul>
    </div>
</div>
"""''', code, flags=re.DOTALL)

with open('pipeline/src/brain/dynamic_insight.py', 'w', encoding='utf-8') as f:
    f.write(new_code)

import glob, re

pattern_paper = re.compile(r',\s*paper_bgcolor=[\"\'][A-Za-z0-9#]+[\"\']')
pattern_plot = re.compile(r',\s*plot_bgcolor=[\"\'][A-Za-z0-9#]+[\"\']')
pattern_font = re.compile(r',\s*font=dict\(color=[\"\'][A-Za-z0-9#]+[\"\'](?:,\s*size=\d+)?\)')
pattern_legend = re.compile(r',\s*legend=dict\(.*?bgcolor=[\"\']rgba\(0,\s*0,\s*0,\s*0\)[\"\']\)')
pattern_iso_legend = re.compile(r'legend=dict\(bgcolor=[\"\']rgba\(0,\s*0,\s*0,\s*0\)[\"\']\)')

for f in glob.glob('pipeline/pages/*.py') + glob.glob('pipeline/*.py'):
    with open(f, 'r', encoding='utf-8') as file:
        content = file.read()
    
    old = content
    content = pattern_paper.sub('', content)
    content = pattern_plot.sub('', content)
    content = pattern_font.sub('', content)
    content = pattern_legend.sub('', content)
    content = pattern_iso_legend.sub('legend=dict()', content)
    
    if content != old:
        with open(f, 'w', encoding='utf-8') as file:
            file.write(content)
        print('Removed hardcoded dark bg in', f)

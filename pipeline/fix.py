import glob
for f in glob.glob('pipeline/pages/*.py')+glob.glob('pipeline/*.py'):
    with open(f, 'r', encoding='utf-8') as file:
        content = file.read()
    
    if r'"rerun"' in content:
        content = content.replace(r'"rerun"', '"rerun"')
        with open(f, 'w', encoding='utf-8') as file:
            file.write(content)
        print('Fixed:', f)

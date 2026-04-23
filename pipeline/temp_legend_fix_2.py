import glob, re

def update_legends():
    count_files = 0
    for f in glob.glob('pipeline/pages/*.py') + glob.glob('pipeline/*.py'):
        try:
            with open(f, 'r', encoding='utf-8') as file:
                content = file.read()
            
            # Simple global substitution using regex to catch trailing parens across newlines
            new_content = content.replace("theme=None", "theme=None")
            
            # Replace: `width='stretch'` followed by optional spaces, then `)`
            new_content = re.sub(r'width='stretch'\s*,?\s*\)', r'width='stretch')', new_content)
            
            # Catch charts where it ends with on_select="rerun"
            new_content = re.sub(r'on_select="rerun"\s*,?\s*\)', r'on_select="rerun")', new_content)

            # Fix any duplicates that got made
            new_content = new_content.replace(", theme=None", ", theme=None")
            
            if new_content != content:
                with open(f, 'w', encoding='utf-8') as file:
                    file.write(new_content)
                count_files += 1
                print(f"Updated {f}")
        except Exception as e:
            print(f"Error reading {f}: {e}")
    print(f"Total files updated: {count_files}")

if __name__ == "__main__":
    update_legends()

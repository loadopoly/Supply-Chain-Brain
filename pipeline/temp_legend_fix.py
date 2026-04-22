import glob, re

def update_legends():
    count_files = 0
    for f in glob.glob('pipeline/pages/*.py') + glob.glob('pipeline/*.py'):
        try:
            with open(f, 'r', encoding='utf-8') as file:
                content = file.read()
            
            # Simple replace without regex
            new_content = content.replace("use_container_width=True)", "use_container_width=True)")
            new_content = new_content.replace(")", ")")
            
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






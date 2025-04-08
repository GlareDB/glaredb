import os
import re
import sys

LOWERCASE_WORDS = {
    'a', 'an', 'the',  # articles
    'and', 'but', 'or', 'nor', 'for',  # coordinating conjunctions
    'in', 'to', 'of', 'at', 'by', 'for', 'from', 'with', 'on', 'as'  # prepositions
}

def title_case(text):
    """
    Convert text to Title Case, preserving code blocks and other formatting.
    """
    if '`' in text:
        parts = []
        in_backticks = False
        for part in text.split('`'):
            if in_backticks:
                parts.append('`' + part + '`')
            else:
                if part:
                    parts.append(apply_title_case(part))
            in_backticks = not in_backticks
        return ''.join(parts)
    else:
        return apply_title_case(text)

def apply_title_case(text):
    """
    Apply title case rules to text without any formatting.
    """
    words = text.split()
    if not words:
        return text
    
    words[0] = capitalize_word(words[0])
    if len(words) > 1:
        words[-1] = capitalize_word(words[-1])
    
    for i in range(1, len(words) - 1):
        word = words[i]
        if word.lower() in LOWERCASE_WORDS:
            words[i] = word.lower()
        else:
            words[i] = capitalize_word(word)
    
    return ' '.join(words)

def capitalize_word(word):
    """
    Capitalize a word, preserving any special characters.
    """
    if not word:
        return word
    return word[0].upper() + word[1:]

def process_file(file_path):
    """
    Process a Markdown file to update headers to Title Case.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    modified = False
    
    frontmatter_match = re.search(r'---\s+title: (.*?)(\s+.*?)?---', content, re.DOTALL)
    if frontmatter_match:
        title = frontmatter_match.group(1).strip()
        title_cased = title_case(title)
        if title != title_cased:
            content = content.replace(f'title: {title}', f'title: {title_cased}')
            modified = True

    header_pattern = re.compile(r'^(#{1,3}) (.*?)$', re.MULTILINE)
    
    def replace_header(match):
        nonlocal modified
        prefix = match.group(1)
        header_text = match.group(2)
        header_text_cased = title_case(header_text)
        if header_text != header_text_cased:
            modified = True
            return f'{prefix} {header_text_cased}'
        return match.group(0)
    
    content = header_pattern.sub(replace_header, content)
    
    if modified:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Updated: {file_path}")
        return True
    return False

def find_md_files(directory):
    """
    Find all Markdown files in the given directory and its subdirectories.
    """
    md_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.md'):
                md_files.append(os.path.join(root, file))
    return md_files

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fix_title_case.py <directory>")
        sys.exit(1)
    
    directory = sys.argv[1]
    md_files = find_md_files(directory)
    updated_count = 0
    
    for file_path in md_files:
        if process_file(file_path):
            updated_count += 1
    
    print(f"Processed {len(md_files)} files, updated {updated_count} files.")
